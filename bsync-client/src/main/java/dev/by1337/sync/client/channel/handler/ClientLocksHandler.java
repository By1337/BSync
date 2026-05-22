package dev.by1337.sync.client.channel.handler;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.IncomingRequest;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CForceUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public abstract class ClientLocksHandler implements ChannelHandler {
    private final AtomicInteger counter = new AtomicInteger();
    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private final Map<UUID, LockData> locks = new ConcurrentHashMap<>();
    private Connection freedom;
    private Pipeline pipeline;
    private boolean closed;

    @Override
    public final void init(ChannelRuntime runtime) {
        if (!(runtime instanceof ClientChannelRuntime ccr)) throw new IllegalArgumentException("Invalid runtime type");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        freedom = ccr.freedom();
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        eventLoop.schedule(this::tick, 10_000);
    }

    private void tick() {
        if (closed) return;
        for (var lock : List.copyOf(locks.values())) {
            if (lock.pending) continue;
            freedom.write(new C2SRenewLockPacket(lock.key, lock.token));
        }
        eventLoop.schedule(this::tick, 10_000);
    }

    public Logger getLogger() {
        return log;
    }

    public boolean isLocked(UUID uuid) {
        var v = locks.get(uuid);
        return v != null && !v.isPending();
    }

    protected abstract byte[] forceUnlockNow(UUID key);

    protected abstract void onMailAccept(UUID key, String json);

    @Override
    public final void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof S2CForceUnlockPacket unlock) {
            var lock = locks.get(unlock.key());
            if (lock == null || unlock.token() != lock.token) return; //outdated
            locks.remove(unlock.key());
            byte[] arr = forceUnlockNow(unlock.key());
            log.error("Force unlocked by server {}! DATA LOST {}", unlock.key(), arrayToBase64(arr));
        } else if (msg instanceof IncomingRequest r) {
            if (r.payload() instanceof S2CMailAcceptPacket mail) {
                var lock = locks.get(mail.key());
                if (lock == null || lock.token != mail.token()) {
                    r.response(mail, C2SMailResponsePacket.reject(-1));
                    return; //outdated
                }
                r.response(mail, C2SMailResponsePacket.accepted(lock.token));
                try {
                    onMailAccept(mail.key(), mail.json());
                } catch (Exception e) {
                    log.error("Failed to accept mail {}", mail, e);
                }
            } /*else if (r.payload() instanceof S2CLockStatusRequestPacket status) {
                var lock = locks.get(status.key());
                if (lock == null) {
                    r.response(status, C2SLockStatusResponsePacket.free());
                }
                //todo игнорим ответ?
                if (lock == null || lock.token != status.token()) return; //outdated

            }*/
        } else {
            if (msg instanceof ChannelActiveMessage) {
/*                for (var lock : List.copyOf(locks.entrySet())) {
                    pipeline.execute(RequestsHandler.request(
                            new C2SRelockRequestPacket(lock.getKey(), ++lock.getValue().ct),
                            (status, con) -> {
                                var l = locks.get(lock.getKey());
                                if (status == null){
                                    //нам не ответили...
                                }
                                if (l == null){
                                    //мы не держим блокировку...
                                }
                                if (l.token != status.token()){
                                    //эпохи не совпадают
                                }
                                if (l.ct != status.ct()){
                                    //
                                }
                        if (status == null || status.isRejected()) {
                            locks.remove(lock);
                            byte[] arr = forceUnlockNow(lock);
                            log.error("Failed to reget lock for {}! DATA LOST {} {}", lock, arrayToBase64(arr), status);
                        }
                    }, 15_000), freedom);
                }*/
            } else if (msg instanceof ChannelInactiveMessage) {
                log.warn("Connection lost maybe data lost!");
            }
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
        eventLoop.assertThread();
        closed = true;
        for (var lock : List.copyOf(locks.entrySet())) {
            try {
                var data = lock.getValue();
                var key = lock.getKey();
                byte[] blob = forceUnlockNow(lock.getKey());
                if (data.isPending()) {
                    if (blob != null) {
                        log.error("Trying to flush data without lock! {} DATA LOST {}", key, arrayToBase64(blob));
                    }
                    continue;
                }
                if (blob == null) {
                    freedom.write(new C2SUnlockPacket(key, data.token));
                } else {
                    freedom.write(new C2SUnlockAndFlushBlobPacket(key, blob, data.token));
                }
            } catch (Exception e) {
                log.error("Failed to flush data for {}", lock, e);
            }
        }
    }

    public void pushMail(UUID key, String json) {
        eventLoop.execute(() -> {
            if (!isLocked(key)) {
                freedom.write(new C2SPushMailPacket(key, json));
            } else {
                try {
                    onMailAccept(key, json);
                } catch (Exception e) {
                    log.error("Failed to accept mail {} {}", key, json, e);
                }
            }
        });
    }

    public void unlock(UUID key, byte @Nullable [] blob) {
        unlock(key, blob, () -> {});
    }

    public void unlock(UUID key, byte @Nullable [] blob, Runnable r) {
        eventLoop.execute(() -> {
            var lock = locks.remove(key);
            r.run();
            if (lock == null) {
                log.error("Failed to unlock {} DATA LOST {}", key, arrayToBase64(blob));
                return;
            }
            if (lock.isPending()) {
                if (blob != null) {
                    log.error("Trying to flush data without lock! {} DATA LOST {}", key, arrayToBase64(blob));
                }
            } else {
                if (blob == null) {
                    freedom.write(new C2SUnlockPacket(key, lock.token));
                } else {
                    freedom.write(new C2SUnlockAndFlushBlobPacket(key, blob, lock.token));
                }
            }
        });
    }

    private String arrayToBase64(byte[] arr) {
        return arr == null ? "null" : Base64.getEncoder().encodeToString(arr);
    }

    // 2 lockAndLoadData - побеждает первый
    // lockAndLoadData - во время наличия блокировки не возможен
    public void lockAndLoadData(UUID key, BiConsumer<LockStatus, byte @Nullable []> callback) {
        var lock = new LockData(counter.getAndIncrement(), key);
        // Вне eventLoop только здесь вызываю ConcurrentHashMap#put.
        // Скажем что вне eventLoop можно put только если изначально там null.
        if (locks.putIfAbsent(key, lock) != null) {
            throw new IllegalStateException("Key " + key + " is already locked!");
        }
        pipeline.schedule(RequestsHandler.request(
                new C2SLockAndGetBlobRequestPacket(key, lock.version),
                (status, conn) -> {
                    var actualLock = locks.get(key);
                    if (status == null) {
                        log.error("Failed to get lock for {} Has no response", key);
                        locks.remove(key, lock);
                        callback.accept(LockStatus.FAILURE, null);
                    } else {
                        if (actualLock == null || actualLock.version != status.version()) {
                            log.error("Failed to get lock for {} Outdated response {}", key, status);
                            conn.write(new C2SUnlockPacket(key, status.token()));
                            callback.accept(LockStatus.FAILURE, null);
                        } else {
                            if (status.isAccepted()) {
                                actualLock.token = status.token();
                                actualLock.pending = false;
                                callback.accept(LockStatus.SUCCESS, status.blob());
                                //callback может вызвать unlock
                                if (isLocked(key)) {
                                    conn.write(new C2SPollAllMailsPacket(key, status.token()));
                                }
                            } else {
                                log.error("Failed to get lock for {} response {}", key, status);
                                callback.accept(LockStatus.FAILURE, null);
                                locks.remove(key, lock);
                            }
                        }
                    }
                },
                15_000
        ), freedom, 100);
    }

    public enum LockStatus {
        SUCCESS,
        FAILURE
    }

    private static class LockData {
        private int token;
        private int version;
        private boolean pending = true;
        private long ownershipUntil;
        private final UUID key;

        public LockData(int version, UUID key) {
            this.version = version;
            this.key = key;
        }

        public boolean isPending() {
            return pending;
        }
    }
}
