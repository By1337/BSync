package dev.by1337.sync.server.channel.handler;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.IncomingRequest;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CForceUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusAndBlobPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import dev.by1337.sync.server.database.K2VCache;
import dev.by1337.sync.server.database.table.UuidBlobRepository;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerLockHandler implements ChannelHandler {

    private int mailIds = 0;
    private final Map<UUID, LinkedHashMap<Integer, String>> mails = new HashMap<>();
    private final LockMap lockMap = new LockMap();

    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private Pipeline pipeline;
    private ServerChannelRuntime serverChannel;
    private boolean closing = false;
    private K2VCache<UUID, byte[]> blobRepository;
    private DedicatedServer server;

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        serverChannel = runtime;
        eventLoop.schedule(this::tick, 10_000);
        server = runtime.server();
        blobRepository = new K2VCache<>(
                new UuidBlobRepository(server.database().dataSource(), runtime.channel().id() + "_blob_repository")
        );
    }

    private void tick() {
        long now = System.currentTimeMillis();
        for (var lock : List.copyOf(lockMap.key2lock.values())) {
            if (lock.lastConfirm + LockMap.OUTDATE_TIME_MS < now) {
                lockMap.unlock(lock.key);
                var v = serverChannel.lookup(lock.owner);
                if (v != null) v.write(new S2CForceUnlockPacket(lock.key, lock.token));
            }
        }
        eventLoop.schedule(this::tick, 15_000);
    }

    private <T> T safe(ESupplier<T> s) {
        try {
            return s.get();
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
            return null;
        }
    }

    private void safe(ERunnable s) {
        try {
            s.run();
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
        }
    }

    private <T> T safeOptional(ESupplier<Optional<T>> s) {
        try {
            return s.get().orElse(null);
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
            return null;
        }
    }

    @FunctionalInterface
    public interface ESupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    public interface ERunnable {
        void run() throws Exception;
    }


    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        switch (msg) {
            case ClientDisconnectMessage(SocketConnection connection) -> {
                lockMap.markDisconnected(connection);
                ctx.fire(msg);
            }
            case IncomingRequest request -> {
                ChannelMessage payload = request.payload();
                if (payload instanceof C2SLockAndGetBlobRequestPacket r) {
                    // todo badShutdown тут не играет роли пока не научимся нормально закрывать соединения с серверной стороны
                    if (!r.recovery() && /*server.badShutdown() &&*/ server.uptimeMillis() < 3_000) {
                        log.warn("Guard time for key {}", r.key());
                        pipeline.schedule(msg, ctx.connection(), 500);
                        return;
                    }
                    var lock = lockMap.tryLock(r.key(), ctx.connection().transport());
                    if (lock == null) {
                        if (++request.counter >= 10) {
                            request.response(r, new S2CLockStatusAndBlobPacket(
                                    S2CLockStatusAndBlobPacket.Status.REJECTED, null, -1, r.version())
                            );
                        } else {
                            pipeline.schedule(msg, ctx.connection(), 500);
                        }
                    } else {
                        request.response(r, new S2CLockStatusAndBlobPacket(
                                        S2CLockStatusAndBlobPacket.Status.ACCEPTED,
                                        safeOptional(() -> blobRepository.get(r.key())),
                                        lock.token,
                                        r.version()
                                )
                        );
                    }
                } else {
                    ctx.fire(msg);
                }
            }
            case C2SRenewLockPacket(UUID key, int token) -> {
                var lock = lockMap.getLock(key);
                if (lock != null && lock.isOwner(token, ctx.connection().transport())) {
                    lock.lastConfirm = System.currentTimeMillis();
                } else {
                    ctx.connection().write(new S2CForceUnlockPacket(key, token));
                }
            }
            case C2SPollAllMailsPacket(UUID key, int token) -> {
                if (lockMap.isOwner(key, ctx.connection().transport(), token)) {
                    var mails = this.mails.get(key);
                    if (mails == null || mails.isEmpty()) return;
                    sendMail(ctx.connection(), key);
                }
            }
            case C2SPushMailPacket(UUID key, String json) -> {
                int id = mailIds++;
                this.mails.computeIfAbsent(key, k -> new LinkedHashMap<>()).put(id, json);
                var lock = lockMap.getLock(key);
                if (lock != null) {
                    sendMail(serverChannel.lookup(lock.owner), key);
                }
            }
            case C2SFlushBlobPacket flush -> {
                var lock = lockMap.getLock(flush.key());
                if (lock != null && lock.isOwner(flush.token(), ctx.connection().transport()) && flush.version() > lock.snapshotVersion) {
                    lock.snapshotVersion = flush.version();
                    var blob = flush.blob();
                    if (blob != null) {
                        safe(() -> blobRepository.put(lock.key, flush.blob()));
                    }
                }
            }
            case C2SUnlockPacket unlock ->
                    lockMap.unlockIfOwner(unlock.key(), ctx.connection().transport(), unlock.token());
            case null, default -> ctx.fire(msg);
        }
    }


    private void sendMail(Connection connection, UUID key) {
        var lock = lockMap.getLock(key);
        if (lock == null || lock.owner != connection.transport()) return;
        var map = this.mails.get(key);
        if (map == null || map.isEmpty()) return;
        var mail = map.firstEntry();
        RequestsHandler.request(new S2CMailAcceptPacket(key, mail.getValue(), lock.token), (result, conn) -> {
            if (conn != null && result instanceof C2SMailResponsePacket response) {
                if (!lockMap.isOwner(key, conn.transport(), response.token())) {
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
                log.error("Client {} не ответил на S2CMailAcceptPacket {}", conn, result);
            }
        }, 10_000).execute(pipeline, connection);
    }

    @Override
    public void close() {
        closing = true;
    }

    public static class LockData {
        public boolean disconnected;
        public long disconnectedAt;
        public int token;
        public final UUID key;
        public final SocketConnection owner;
        public long lastConfirm;
        public int snapshotVersion;

        public LockData(UUID key, SocketConnection owner, int token) {
            this.key = key;
            this.owner = owner;
            this.token = token;
        }

        public boolean isOwner(int token, SocketConnection c) {
            return this.token == token && c == owner;
        }
    }

    public static class LockMap {
        private final AtomicInteger counter = new AtomicInteger();
        private static final long OUTDATE_TIME_MS = 15_000;
        private final Map<SocketConnection, Set<UUID>> client2keys = new IdentityHashMap<>(1024);
        private final Map<UUID, SocketConnection> key2client = new HashMap<>(1024);
        private final Map<UUID, LockData> key2lock = new HashMap<>(1024);

        public @Nullable LockData getLock(UUID key) {
            return key2lock.get(key);
        }

        public boolean isOwner(UUID key, SocketConnection connection, int token) {
            var lock = key2lock.get(key);
            return lock != null && lock.owner == connection && lock.token == token;
        }

        public Set<UUID> getOwnedKeys(SocketConnection key) {
            return Collections.unmodifiableSet(client2keys.getOrDefault(key, Set.of()));
        }

        public void markDisconnected(SocketConnection c) {
            var set = client2keys.get(c);
            if (set != null) {
                for (UUID uuid : set) {
                    var lock = getLock(uuid);
                    if (lock != null) {
                        lock.disconnected = true;
                        lock.disconnectedAt = System.currentTimeMillis();
                    }
                }
            }
        }

        public Set<UUID> dropAllFor(SocketConnection c) {
            var set = client2keys.remove(c);
            if (set == null) return Set.of();
            key2client.keySet().removeAll(set);
            key2lock.keySet().removeAll(set);
            return Collections.unmodifiableSet(set);
        }

        public void unlockIfOwner(UUID key, SocketConnection connection, int token) {
            var lock = key2lock.get(key);
            if (lock == null) return;
            if (lock.token == token) {
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
                if (old.disconnected && old.disconnectedAt + 2_000 <= System.currentTimeMillis()) {
                    unlock(key);
                    return tryLock(key, c);
                }
                // if (old.owner == c) {
                //     old.lastConfirm = System.currentTimeMillis();
                //     old.token = counter.incrementAndGet();
                //     return old;
                // }
                return null;
            }
            LockData lock = new LockData(key, c, counter.incrementAndGet());
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
