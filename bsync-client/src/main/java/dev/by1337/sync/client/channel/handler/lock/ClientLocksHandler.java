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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class ClientLocksHandler implements ChannelHandler, Locks {
    public static final int MAX_BLOB_SIZE = (16 << 20) - 128;
    private static final long MAIL_REQUEST_TIMEOUT = 5_000;
    private static final long MAIL_RETRY_DELAY = 2_000;
    private static final int MAIL_MAX_RETRIES = 10;
    private static final long MAIL_RETRY_TTL_NANOS = 120_000_000_000L;
    private final AtomicInteger counter = new AtomicInteger();
    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private final Map<UUID, LockData> locks = new ConcurrentHashMap<>();
    private final Map<UUID, byte @NotNull []> recovery = new HashMap<>(256);
    private Connection remote;
    private Pipeline pipeline;
    private boolean closing;
    private boolean ready;
    private LongSet mailsA = new LongOpenHashSet();
    private LongSet mailsB = new LongOpenHashSet();
    private final Queue<PendingMail> pendingMails = new ArrayDeque<>();
    private long nextMailUid;
    private int remainingMailUids;
    private boolean requestingMailUids;
    private boolean mailboxActive;
    private int mailSession;
    private final Locks.Type type;

    private BSUtils.FaultIsolation<LockManager> lockManager;

    public ClientLocksHandler(Type type1) {
        this.type = type1;
    }

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
        eventLoop.repeat(this::rotateDiffs, 60_000 * 5, () -> closing);
        ready = true;
    }

    private void rotateDiffs() {
        var v = mailsB;
        mailsB = mailsA;
        mailsA = v;
        v.clear();
    }

    @Override
    public boolean isReady() {
        return ready;
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
        if (!ready) return false;
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
                if (mailsA.contains(mail.uid()) || mailsB.contains(mail.uid())) return;
                mailsA.add(mail.uid());
                lockManager.run(v -> v.acceptMail(mail.key(), mail.json()));
            }
        } else {
            if (msg instanceof ChannelActiveMessage) {
                mailboxActive = true;
                if (type.hasMailbox) requestMailUids(0);
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
            } else if (msg instanceof ChannelInactiveMessage) {
                mailboxActive = false;
                mailSession++;
                remainingMailUids = 0;
                requestingMailUids = false;
                discardPendingMails();
                if (!locks.isEmpty()) log.error("Connection lost but has locks!");
                for (LockData value : List.copyOf(locks.values())) {
                    lockManager.run(v -> v.forceUnlock(value.key));
                    locks.remove(value.key, value);
                    if (value.snapshot != null)
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
        mailboxActive = false;
        mailSession++;
        discardPendingMails();
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
        if (!type.hasMailbox){
            log.error("mails is not supported! {} {}", key, json);
            return;
        }
        if (!ready || closing) {
            log.error("Failed to push mail channel is not ready! {} {}", key, json);
            return;
        }
        eventLoop.execute(() -> {
            if (closing) return;
            if (!isLocked(key)) {
                if (mailboxActive && remainingMailUids > 0) {
                    sendNewMail(key, json);
                } else {
                    pendingMails.offer(new PendingMail(key, json));
                    flushPendingMails();
                }
            } else {
                lockManager.run(v -> v.acceptMail(key, json));
            }
        });
    }

    private void flushPendingMails() {
        if (closing || !mailboxActive) return;
        while (remainingMailUids > 0 && !pendingMails.isEmpty()) {
            var mail = pendingMails.remove();
            sendNewMail(mail.key(), mail.json());
        }
        if (!pendingMails.isEmpty()) requestMailUids(0);
    }

    private void sendNewMail(UUID key, String json) {
        remainingMailUids--;
        var packet = new C2SPushMailPacket(key, json, nextMailUid++);
        sendMail(packet, 0, mailSession, System.nanoTime() + MAIL_RETRY_TTL_NANOS);
    }

    private void requestMailUids(int retries) {
        if (closing || !mailboxActive || requestingMailUids) return;
        requestingMailUids = true;
        int session = mailSession;
        new C2SGiveMailUidsRequest().request(pipeline, remote, MAIL_REQUEST_TIMEOUT).then(response -> {
            if (closing || session != mailSession || !mailboxActive) return;
            if (response == null) {
                if (retries >= MAIL_MAX_RETRIES) {
                    requestingMailUids = false;
                    log.error("Failed to obtain mailbox ID range after {} attempts", retries + 1);
                    discardPendingMails();
                } else {
                    eventLoop.schedule(() -> {
                        if (closing || session != mailSession || !mailboxActive) return;
                        requestingMailUids = false;
                        requestMailUids(retries + 1);
                    }, MAIL_RETRY_DELAY);
                }
                return;
            }
            requestingMailUids = false;
            nextMailUid = response.value();
            remainingMailUids = C2SGiveMailUidsRequest.RANGE_SIZE;
            flushPendingMails();
        });
    }

    private void sendMail(C2SPushMailPacket packet, int retries, int session, long deadline) {
        if (closing || session != mailSession || !mailboxActive || System.nanoTime() >= deadline) {
            log.error("Mail push was not confirmed: {}", packet);
            return;
        }
        packet.request(pipeline, remote, MAIL_REQUEST_TIMEOUT).then(response -> {
            if (response != null && response.flag()) return;
            if (closing || session != mailSession || !mailboxActive
                    || retries >= MAIL_MAX_RETRIES || System.nanoTime() >= deadline) {
                log.error("Failed to confirm mail push after {} attempts: {}", retries + 1, packet);
                return;
            }
            eventLoop.schedule(() -> sendMail(packet, retries + 1, session, deadline), MAIL_RETRY_DELAY);
        });
    }

    private void discardPendingMails() {
        PendingMail mail;
        while ((mail = pendingMails.poll()) != null) {
            log.error("Mail push was not sent: {}", mail);
        }
    }

    private record PendingMail(UUID key, String json) {}

    public void pushSnapshot(UUID key, byte @Nullable [] snapshot) {
        if (snapshot == null) return;
        if (!ready) {
            log.error("Failed to push snapshot channel is not ready! {} {}", key, arrayToBase64(snapshot));
            return;
        }
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

    @Override
    public CompletableFuture<byte @Nullable []> loadSnapshot(UUID key) {
        if (!ready || closing) {
            return CompletableFuture.failedFuture(new IllegalStateException("Lock channel is not ready"));
        }
        CompletableFuture<byte @Nullable []> result = new CompletableFuture<>();
        new C2SLoadSnapshotPacket(key).request(pipeline, remote).then(response -> {
            if (response == null) {
                result.completeExceptionally(new TimeoutException("No snapshot response for " + key));
            } else {
                result.complete(response.snapshot());
            }
        });
        return result;
    }

    private void sendSnapshot(UUID key, byte[] snapshot, int token, int version, int counter) {
        new C2SFlushBlobPacket(key, token, version, snapshot).withAck(pipeline, remote)
                .then(state -> {
                    if (!isLocked(key)) return;
                    if (state == null || !state) {
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
        if (!ready) return;
        eventLoop.execute(() -> {
            var lock = locks.get(key);
            if (lock == null) {
                log.error("Failed to unlock {} key is not locked", key);
                return;
            }
            if (version != -1 && lock.version != version) return;
            if (lock.isPending()) {
                locks.remove(key);
            } else if (closing || lock.snapshot == null) {
                locks.remove(key, lock);
                if (lock.snapshot != null)
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

    @Override
    public void loadMails(UUID key) {
        if (!ready) return;
        if (!type.hasMailbox) return;
        eventLoop.execute(() -> {
            var lock = locks.get(key);
            if (lock == null || lock.isPending()) return;
            remote.write(new C2SPollAllMailsPacket(key, lock.token));
        });
    }

    // 2 lockAndLoadData - побеждает первый
    // lockAndLoadData - во время наличия блокировки не возможен
    @Override
    public int lockAndLoadData(UUID key, BiConsumer<Locks.LockStatus, byte @Nullable []> callback) {
        return lockAndLoadData(key, BSUtils.faultIsolation(callback));
    }

    private int lockAndLoadData(UUID key, BSUtils.FaultIsolation<BiConsumer<LockStatus, byte @Nullable []>> callback) {
        if (!ready) {
            callback.run(v -> v.accept(LockStatus.FAILURE, null));
            return 0;
        }
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
