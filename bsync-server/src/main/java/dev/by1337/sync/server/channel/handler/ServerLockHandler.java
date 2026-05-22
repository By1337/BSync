package dev.by1337.sync.server.channel.handler;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.IncomingRequest;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
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
    private boolean closed = false;

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
        eventLoop.schedule(this::tick, 10_000);
    }

    private void tick() {
        long now = System.currentTimeMillis();
        for (var lock : List.copyOf(lockMap.key2lock.values())) {
            if (lock.lastConfirm + LockMap.OUTDATE_TIME_MS < now) {
                lockMap.unlock(lock.key);
                serverChannel.lookup(lock.owner).write(new S2CForceUnlockPacket(lock.key));
            } else if (lock.lastConfirm + 10_000 < now) {
                ensureLockOwnership(lock.key);
            }
        }
        eventLoop.schedule(this::tick, 15_000);
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof ClientDisconnectMessage(SocketConnection connection)) {
            var s = lockMap.dropAllFor(connection);
            if (!s.isEmpty()) {
                log.error("Client {} disconnected but has locks! {}", connection, s);
            }
            ctx.fire(msg);
        } else if (msg instanceof IncomingRequest request) {
            ChannelMessage payload = request.payload();
            if (payload instanceof C2SLockAndGetBlobRequestPacket r) {
                var lock = lockMap.tryLock(r.key, ctx.connection().transport());
                if (lock == null) {
                    if (++request.counter >= 10) {
                        request.response(r, new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.REJECTED, null));
                    } else {
                        pipeline.schedule(msg, ctx.connection(), 500);
                    }
                } else {
                    request.response(r, new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.ACCEPTED, database.get(r.key)));
                    ensureLockOwnership(r.key);
                }
            } else if (payload instanceof C2SRelockRequestPacket relock) {
                var lock = lockMap.tryLock(relock.key, ctx.connection().transport());
                if (lock == null) {
                    request.response(relock, S2CLockStatusResponsePacket.reject());
                } else {
                    request.response(relock, S2CLockStatusResponsePacket.owned());
                }
            } else if (payload instanceof C2SLockStatusReqestPacket s) {
                if (lockMap.isOwner(s.key, ctx.connection().transport())) {
                    request.response(s, S2CLockStatusResponsePacket.owned());
                } else {
                    request.response(s, S2CLockStatusResponsePacket.reject());
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
                log.error("Клиент {} пишет в {} без блокировки! DATA LOST {}", ctx.connection().transport(), flush.key, arrayToBase64(flush.blob));
                return;
            }
            database.put(flush.key, flush.blob);
            lockMap.unlock(flush.key);
        } else if (msg instanceof C2SUnlockPacket unlock) {
            lockMap.unlockIfOwner(unlock.key, ctx.connection().transport());
        } else {
            ctx.fire(msg);
        }
    }

    private void ensureLockOwnership(UUID key) {
        var lock0 = lockMap.getLock(key);
        if (lock0 == null) return;
        var connection = serverChannel.lookup(lock0.owner);
        pipeline.execute(RequestsHandler.request(new S2CLockStatusRequestPacket(key), (status, conn) -> {
            if (status != null) {
                if (status.isLocked()) {
                    if (lockMap.tryLock(key, conn.transport()) == null) {
                        conn.write(new S2CForceUnlockPacket(key));
                    }
                } else {
                    lockMap.unlockIfOwner(key, conn.transport());
                }
            } else {
                lockMap.unlockIfOwner(key, conn.transport());
                log.error("Bad response for S2CLockStatusRequestPacket {} {}", status, conn);
            }
        }, 10_000), connection);
    }

    private void sendMail(Connection connection, UUID key) {
        var map = this.mails.get(key);
        if (map == null || map.isEmpty()) return;
        var mail = map.firstEntry();
        pipeline.execute(RequestsHandler.request(new S2CMailAcceptPacket(key, mail.getValue()), (result, conn) -> {
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
        closed = true;
    }

    public static class LockData {
        public int version;
        public final UUID key;
        public final SocketConnection owner;
        public long lastConfirm;

        public LockData(UUID key, SocketConnection owner, int version) {
            this.key = key;
            this.owner = owner;
            this.version = version;
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

        public void unlockIfOwner(UUID key, SocketConnection c) {
            var lock = key2lock.get(key);
            if (lock == null) return;
            if (lock.owner == c) {
                unlock(key);
            }
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
            LockData lock = new LockData(key, c, 0);
            lock.lastConfirm = System.currentTimeMillis();
            key2lock.put(key, lock);
            key2client.put(key, c);
            client2keys.computeIfAbsent(c, k -> new HashSet<>()).add(key);
            return lock;
        }

    }

    private String arrayToBase64(byte[] arr) {
        return arr == null ? "null" : Base64.getEncoder().encodeToString(arr);
    }
}
