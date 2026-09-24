package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.GetPostChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.impl.c2s.C2SLoadSnapshotPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SLockAndGetBlobRequestPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SRenewLockPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CForceUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusAndBlobPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CSnapshotPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerLockerHandler extends GetPostChannelHandler {

    private final LockMap lockMap = new LockMap();
    private boolean closing = false;
    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private Pipeline pipeline;
    private ServerChannelRuntime serverChannel;
    private DedicatedServer server;
    private @Nullable ServerBlobRepoHandler blobs;

    public ServerLockerHandler() {
        registerSoftPost(ClientDisconnectMessage.class, msg -> {
            lockMap.markDisconnected(msg.connection());
        });
        registerPost(C2SRenewLockPacket.class, (ctx, renew) -> {
            UUID key = renew.key();
            int token = renew.token();
            var lock = lockMap.getLock(key);
            if (lock != null && lock.isOwner(token, ctx.connection().transport())) {
                lock.lastConfirm = System.currentTimeMillis();
            } else {
                ctx.connection().write(new S2CForceUnlockPacket(key, token));
            }
        });
        registerPost(C2SUnlockPacket.class, (ctx, unlock) -> {
            lockMap.unlockIfOwner(unlock.key(), ctx.connection().transport(), unlock.token());
        });
        registerGet(C2SLockAndGetBlobRequestPacket.class, (r, conn) -> {
            ResponseFuture<S2CLockStatusAndBlobPacket> future = new ResponseFuture<>();
            lockAndGetBlob(future, r, conn, 10);
            return future;
        });
    }
    private void lockAndGetBlob(ResponseFuture<S2CLockStatusAndBlobPacket> future, C2SLockAndGetBlobRequestPacket r, Connection conn, int ttl){
        if (!r.recovery() && server.uptimeMillis() < 3_000) {
            log.warn("Guard time for key {}", r.key());
            eventLoop.schedule(() -> lockAndGetBlob(future, r, conn, ttl -1), 500);
            return;
        }
        var lock = lockMap.tryLock(r.key(), conn.transport());
        if (lock == null) {
            if (ttl <= 0) {
                future.complete(new S2CLockStatusAndBlobPacket(
                        S2CLockStatusAndBlobPacket.Status.REJECTED, null, -1, r.version())
                );
            } else {
                eventLoop.schedule(() -> lockAndGetBlob(future, r, conn, ttl -1), 500);
            }
        } else {
            future.complete(new S2CLockStatusAndBlobPacket(
                            S2CLockStatusAndBlobPacket.Status.ACCEPTED,
                            r.recovery() || blobs == null ? null : blobs.loadBlob(r.key()),
                            lock.token,
                            r.version()
                    )
            );
        }
    }

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        serverChannel = runtime;
        server = runtime.server();
        blobs = pipeline.getIfExist(ServerBlobRepoHandler.class);
        eventLoop.repeat(this::tick, 15_000, () -> closing);
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
    }

    @Override
    public void close() {
        closing = true;
    }

    LockData getLock(UUID key) {
        return lockMap.getLock(key);
    }

    public boolean isOwner(UUID key, SocketConnection transport, int token) {
        return lockMap.isOwner(key, transport, token);
    }

    public static class LockData {
        public boolean disconnected;
        public long disconnectedAt;
        public int token;
        public final UUID key;
        public final SocketConnection owner;
        public long lastConfirm;
        public int snapshotVersion;
        public boolean isMailProcess = false;

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
        //private final Map<UUID, SocketConnection> key2client = new HashMap<>(1024);
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
            //key2client.keySet().removeAll(set);
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
            //key2client.remove(key);
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
            //key2client.put(key, c);
            client2keys.computeIfAbsent(c, k -> new HashSet<>()).add(key);
            return lock;
        }

    }
}
