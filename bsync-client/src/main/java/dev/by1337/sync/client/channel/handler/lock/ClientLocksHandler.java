package dev.by1337.sync.client.channel.handler.lock;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.IncomingRequest;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CForceUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.util.BSUtils;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class ClientLocksHandler implements ChannelHandler, Locks {
    public static final int MAX_BLOB_SIZE = (16 << 20) - 128;
    private final AtomicInteger counter = new AtomicInteger();
    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private final Map<UUID, LockData> locks = new ConcurrentHashMap<>();
    private final Map<UUID, byte[]> recovery = new HashMap<>(256);
    private Connection remote;
    private Pipeline pipeline;
    private boolean closing;

    private BSUtils.FaultIsolation<LockManager> lockManager;

    public void lockManager(LockManager lockManager) {
        this.lockManager = BSUtils.faultIsolation(lockManager);
    }

    @Override
    public void init(ChannelRuntime runtime) {
        if (!(runtime instanceof ClientChannelRuntime ccr)) throw new IllegalArgumentException("Invalid runtime type");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        remote = ccr.remote();
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        eventLoop.schedule(this::tick, 3_000);
    }

    private void tick() {
        if (closing) return;
        for (var lock : List.copyOf(locks.values())) {
            if (lock.pending) continue;
            if (Boolean.TRUE.equals(lockManager.get(v -> v.ensureLockOwnership(lock.key)))) {
                lock.apiDiscards = 0;
                remote.write(new C2SRenewLockPacket(lock.key, lock.token));
            } else if (++lock.apiDiscards > 3) {
                unlock(lock.key, lock.version);
            }
        }
        eventLoop.schedule(this::tick, 3_000);
    }


    public Logger getLogger() {
        return log;
    }

    @Override
    public boolean isLocked(UUID uuid) {
        var v = locks.get(uuid);
        return v != null && !v.isPending();
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof S2CForceUnlockPacket unlock) {
            var lock = locks.get(unlock.key());
            if (lock == null || unlock.token() != lock.token) return; //outdated
            locks.remove(unlock.key(), lock);
            lockManager.run(v -> v.forceUnlock(unlock.key()));
            log.error("Force unlocked by server {}!", unlock.key());
        } else if (msg instanceof IncomingRequest r) {
            if (r.payload() instanceof S2CMailAcceptPacket mail) {
                var lock = locks.get(mail.key());
                if (lock == null || lock.token != mail.token()) {
                    r.response(mail, C2SMailResponsePacket.reject(-1));
                    return; //outdated
                }
                r.response(mail, C2SMailResponsePacket.accepted(lock.token));
                lockManager.run(v -> v.acceptMail(mail.key(), mail.json()));
            }
        } else {
            if (msg instanceof ChannelActiveMessage) {
                if (!recovery.isEmpty()) {
                    log.warn("Trying to recovery locks! {}", recovery.size());
                    for (Map.Entry<UUID, byte[]> entry : recovery.entrySet()) {
                        UUID key = entry.getKey();
                        byte[] blob = entry.getValue();
                        new C2SLockAndGetBlobRequestPacket(entry.getKey(), 1, true).request(pipeline, remote)
                                .then(status -> {
                                    if (status == null || status.isRejected()) {
                                        log.error("Failed to recovery lock {} DATA LOST {}", key, arrayToBase64(blob));
                                        return;
                                    }
                                    remote.write(new C2SFlushBlobPacket(key, status.token(), 1, blob));
                                    remote.write(new C2SUnlockPacket(key, status.token()));
                                });
                    }
                    recovery.clear();
                }
            } else if (msg instanceof ChannelInactiveMessage && !locks.isEmpty()) {
                log.error("Connection lost but has locks!");
                for (LockData value : List.copyOf(locks.values())) {
                    lockManager.run(v -> v.forceUnlock(value.key));
                    locks.remove(value.key, value);
                    recovery.put(value.key, value.snapshot);
                }
            }
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
        eventLoop.assertThread();
        closing = true;
        for (var lock : List.copyOf(locks.entrySet())) {
            try {
                var data = lock.getValue();
                var key = lock.getKey();
                lockManager.run(v -> v.forceUnlock(key));
                unlock(key, data.version);
            } catch (Exception e) {
                log.error("Failed to flush data for {}", lock, e);
            }
        }
        lockManager.run(LockManager::close);
        recovery.clear();
    }

    @Override
    public void pushMail(UUID key, String json) {
        eventLoop.execute(() -> {
            if (!isLocked(key)) {
                //todo дедубликация и ретраи?
                remote.write(new C2SPushMailPacket(key, json));
            } else {
                lockManager.run(v -> v.acceptMail(key, json));
            }
        });
    }

    public void pushSnapshot(UUID key, byte[] snapshot) {
        if (snapshot.length >= MAX_BLOB_SIZE) {
            throw new IllegalArgumentException("Snapshot is too big!");
        }
        eventLoop.execute(() -> {
            var lock = locks.get(key);
            if (lock == null) {
                // этот ключ мог попасть в recovery только если произошёл дисконект от мастер-сервера когда у нас была блокировка
                // pushSnapshot мог прийти из player quit ивента
                // вообще отключение от мастер-сервера провоцирует pushSnapshot, но финальное отключения игрока с сервера происходит не сразу,
                // и если апи имеет более актуальные данные то разрешаем их записать.
                if (recovery.containsKey(key)) {
                    recovery.put(key, snapshot);
                } else {
                    log.error("pushSnapshot for unlocked key! {} {}", key, arrayToBase64(snapshot));
                }
                return;
            }
            lock.snapshot = snapshot;
            lock.snapshotVersion++;
            if (closing) return;
            eventLoop.schedule(() -> {
                if (!isLocked(key)) return;
                sendSnapshot(key, snapshot, lock.token, lock.snapshotVersion, 0);
            }, 100);
        });
    }

    private void sendSnapshot(UUID key, byte[] snapshot, int token, int version, int counter) {
        new C2SFlushBlobPacket(key, token, version, snapshot).withAck(pipeline, remote)
                .then(state -> {
                    if (!isLocked(key)) return;
                    if (!state) {
                        if (counter >= 10) {
                            log.error("Failed to flush {} DATA LOST {}", key, arrayToBase64(snapshot));
                        } else {
                            eventLoop.schedule(() -> sendSnapshot(key, snapshot, token, version, counter + 1), 2000);
                        }
                    }
                });
    }

    public void unlock(UUID key) {
        unlock(key, -1);
    }

    @Override
    public void unlock(UUID key, int version) {
        eventLoop.execute(() -> {
            var lock = locks.get(key);
            if (lock == null) {
                log.error("Failed to unlock {} key is not locked", key);
                return;
            }
            if (version != -1 && lock.version != version) return;
            if (lock.isPending()) {
                locks.remove(key);
            } else if (closing) {
                locks.remove(key, lock);
                remote.write(new C2SFlushBlobPacket(key, lock.token, lock.snapshotVersion, lock.snapshot));
                remote.write(new C2SUnlockPacket(key, lock.token));
            } else {
                new C2SFlushBlobPacket(key, lock.token, lock.snapshotVersion, lock.snapshot).withAck(pipeline, remote)
                        .then(state -> {
                            if (Boolean.FALSE.equals(state)) {
                                log.error("DATA LOST! Server is not accepted blob {} {}", key, arrayToBase64(lock.snapshot));
                            }
                            locks.remove(key, lock);
                            remote.write(new C2SUnlockPacket(key, lock.token));
                        });
            }
        });
    }

    private String arrayToBase64(byte[] arr) {
        return arr == null ? "null" : Base64.getEncoder().encodeToString(arr);
    }

    // 2 lockAndLoadData - побеждает первый
    // lockAndLoadData - во время наличия блокировки не возможен
    @Override
    public int lockAndLoadData(UUID key, BiConsumer<Locks.LockStatus, byte @Nullable []> callback) {
        return lockAndLoadData(key, BSUtils.faultIsolation(callback));
    }

    private int lockAndLoadData(UUID key, BSUtils.FaultIsolation<BiConsumer<LockStatus, byte @Nullable []>> callback) {
        var lock = new LockData(counter.getAndIncrement(), key);
        // Вне eventLoop только здесь вызываю ConcurrentHashMap#put.
        // Скажем что вне eventLoop можно put только если изначально там null.
        if (locks.putIfAbsent(key, lock) != null) {
            eventLoop.execute(() -> {
                callback.run(v -> v.accept(Locks.LockStatus.FAILURE, null));
            });
            return lock.version;
        }
        new C2SLockAndGetBlobRequestPacket(key, lock.version, false).request(pipeline, remote)
                .then((status) -> {
                    var actualLock = locks.get(key);
                    if (status == null || remote == null) {
                        log.error("Failed to get lock for {} Has no response", key);
                        locks.remove(key, lock);
                        callback.run(v -> v.accept(Locks.LockStatus.FAILURE, null));
                    } else {
                        if (actualLock == null || actualLock.version != status.version()) {
                            log.error("Failed to get lock for {} Outdated response {}", key, status);
                            remote.write(new C2SUnlockPacket(key, status.token()));
                            callback.run(v -> v.accept(Locks.LockStatus.FAILURE, null));
                        } else {
                            if (status.isAccepted()) {
                                actualLock.token = status.token();
                                actualLock.pending = false;
                                actualLock.snapshot = status.blob();
                                callback.run(v -> v.accept(Locks.LockStatus.SUCCESS, status.blob()));
                                //callback может вызвать unlock
                                if (isLocked(key)) {
                                    remote.write(new C2SPollAllMailsPacket(key, status.token()));
                                }
                            } else {
                                log.error("Failed to get lock for {} response {}", key, status);
                                callback.run(v -> v.accept(Locks.LockStatus.FAILURE, null));
                                locks.remove(key, lock);
                            }
                        }
                    }
                });
        return lock.version;
    }


    private static class LockData {
        private int token;
        private final int version;
        private boolean pending = true;
        private final UUID key;
        private int apiDiscards = 0;
        private byte @Nullable [] snapshot;
        private int snapshotVersion;

        public LockData(int version, UUID key) {
            this.version = version;
            this.key = key;
        }

        public boolean isPending() {
            return pending;
        }
    }
}
