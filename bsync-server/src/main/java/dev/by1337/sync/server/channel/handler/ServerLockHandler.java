package dev.by1337.sync.server.channel.handler;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.IncomingRequest;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusAndBlobPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.*;
import java.util.function.Consumer;

public class ServerLockHandler implements ChannelHandler {
    private final Map<UUID, byte[]> database = new HashMap<>();
    private int mailIds = 0;
    private final Map<UUID, LinkedHashMap<Integer, String>> mails = new HashMap<>();
    private final LockMap lockMap = new LockMap();

    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private Pipeline pipeline;
    private ServerChannelRuntime serverChannel;

    @Override
    public void init(ChannelRuntime runtime) {
        if (!(runtime instanceof ServerChannelRuntime runtime1))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        serverChannel = runtime1;
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof ClientDisconnectMessage(SocketConnection connection)) {
            var s = lockMap.dropAllFor(connection);
            if (!s.isEmpty()) {
                log.error("Client {} disconnected but has locks! {}", connection, s);
            }
            ctx.fire(msg);
        } else if (msg instanceof IncomingRequest(ChannelMessage payload, Consumer<Packet> out)) {
            if (payload instanceof C2SLockAndGetBlobRequestPacket r) {
                var lock = lockMap.tryLock(r.key, ctx.connection().transport());
                if (lock == null) {
                    out.accept(new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.REJECTED, null));
                    return;
                }
                out.accept(new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.ACCEPTED, database.get(r.key)));
            } else if (payload instanceof C2SRelockRequestPacket relock) {
                var lock = lockMap.tryLock(relock.key, ctx.connection().transport());
                if (lock == null) {
                    out.accept(S2CLockStatusPacket.reject());
                } else {
                    out.accept(S2CLockStatusPacket.owned());
                }
            } else {
                ctx.fire(msg);
            }
        } else if (msg instanceof C2SPollAllMailsPacket poll) {
            if (lockMap.isOwner(poll.key, ctx.connection().transport())) {
                var mails = this.mails.get(poll.key);
                if (mails == null || mails.isEmpty()) return;
                sendMail(ctx.connection(), poll.key);
            }
        } else if (msg instanceof C2SPushMailPacket mail) {
            int id = mailIds++;
            this.mails.computeIfAbsent(mail.key, k -> new LinkedHashMap<>()).put(id, mail.json);
            var lock = lockMap.getLock(mail.key);
            if (lock != null) {
                sendMail(serverChannel.lookup(lock.owner), mail.key);
            }
        } else if (msg instanceof C2SUnlockAndFlushBlobPacket flush) {
            var lock = lockMap.getLock(flush.key);
            if (lock == null || lock.owner != ctx.connection().transport()) {
                log.error("Клиент {} пишет в {} без блокировки! DATA LOST {}", ctx.connection().transport(), flush.key, Base64.getEncoder().encodeToString(flush.blob));
                return;
            }
            database.put(flush.key, flush.blob);
            lockMap.unlock(flush.key);
        }
    }

    private void sendMail(Connection connection, UUID key) {
        var map = this.mails.get(key);
        if (map == null || map.isEmpty()) return;
        var mail = map.firstEntry();
        pipeline.handle(RequestsHandler.request(new S2CMailAcceptPacket(key, mail.getValue()), (result, conn) -> {
            if (result instanceof C2SMailResponsePacket response) {
                if (!lockMap.isOwner(key, conn.transport())) {
                    log.error("Клиент {} принял mail без блокировки! {} {}", conn.transport(), mail, response);
                    return;
                }
                if (response.isAccepted()) {
                    var mails = this.mails.get(key);
                    if (mails == null || mails.remove(mail.getKey()) == null) {
                        log.error("Клиент {} принял не существующий mail {}", connection, mail);
                    }
                    if (mails != null) {
                        if (!mails.isEmpty()) {
                            sendMail(conn, key);
                        } else {
                            this.mails.remove(key);
                        }
                    }
                }
            } else {
                log.error("Client {} не ответил на S2CMailAcceptPacket {}", conn.transport(), result);
            }
        }, 15_000), connection);
    }

    @Override
    public void close() {

    }

    public static class LockData {
        public final UUID key;
        public final SocketConnection owner;
        public long lastConfirm;

        public LockData(UUID key, SocketConnection owner) {
            this.key = key;
            this.owner = owner;
        }
    }

    public static class LockMap {
        private static final long OUTDATE_TIME_MS = 30_000;
        private final Map<SocketConnection, Set<UUID>> client2keys = new IdentityHashMap<>(1024);
        private final Map<UUID, SocketConnection> key2client = new HashMap<>(1024);
        private final Map<UUID, LockData> key2lock = new HashMap<>(1024);

        public @Nullable LockData getLock(UUID key) {
            return key2lock.get(key);
        }

        public boolean isOwner(UUID key, SocketConnection connection) {
            var lock = key2lock.get(key);
            return lock != null && lock.owner == connection;
        }

        public Set<UUID> getOwnedKeys(SocketConnection key) {
            return Collections.unmodifiableSet(client2keys.getOrDefault(key, Set.of()));
        }

        public Set<UUID> dropAllFor(SocketConnection c) {
            var set = client2keys.remove(c);
            if (set == null) return Set.of();
            key2client.keySet().removeAll(set);
            key2lock.keySet().removeAll(set);
            return Collections.unmodifiableSet(set);
        }

        public @Nullable LockData unlock(UUID key) {
            var lock = key2lock.remove(key);
            if (lock == null) return null;
            key2client.remove(key);
            var v = client2keys.get(lock.owner);
            if (v != null) v.remove(key);
            return lock;
        }

        public @Nullable LockData tryLock(UUID key, SocketConnection c) {
            var old = key2lock.get(key);
            if (old != null) {
                if (old.owner == c) {
                    old.lastConfirm = System.currentTimeMillis();
                    return old;
                }
                return null;
            }
            LockData lock = new LockData(key, c);
            lock.lastConfirm = System.currentTimeMillis();
            key2lock.put(key, lock);
            key2client.put(key, c);
            client2keys.computeIfAbsent(c, k -> new HashSet<>()).add(key);
            return lock;
        }

    }
}
