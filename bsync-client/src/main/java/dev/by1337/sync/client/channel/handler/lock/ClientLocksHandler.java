package dev.by1337.sync.client.channel.handler.lock;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.IncomingRequest;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.mail.MailJournal;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CForceUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.transfer.ExportSource;
import dev.by1337.sync.common.transfer.TransferEntry;
import dev.by1337.sync.common.transfer.ShardSelection;
import dev.by1337.sync.common.transfer.TransferResult;
import dev.by1337.sync.common.util.BSUtils;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class ClientLocksHandler implements ChannelHandler, Locks {
    public static final int MAX_BLOB_SIZE = (16 << 20) - 128;
    private static final long MAIL_REQUEST_TIMEOUT = 5_000;
    private final MailJournal outbox;
    private final Map<UUID, Object> inFlightMails = new HashMap<>();
    private final Map<UUID, Integer> mailFailures = new HashMap<>();
    private final Map<UUID, Long> nextMailAttempt = new HashMap<>();
    private final AtomicInteger counter = new AtomicInteger();
    private Logger log = DEFAULT_LOGGER;
    private volatile EventLoopWorker eventLoop;
    private final Map<UUID, LockData> locks = new ConcurrentHashMap<>();
    private final Map<UUID, byte @NotNull []> recovery = new HashMap<>(256);
    private Connection remote;
    private Pipeline pipeline;
    private volatile boolean closing;
    private boolean ready;
    private Set<UUID> mailsA = new HashSet<>();
    private Set<UUID> mailsB = new HashSet<>();
    private boolean mailboxActive;
    private final Locks.Type type;

    private BSUtils.FaultIsolation<LockManager> lockManager;

    /** Low-level in-memory transport; use ChannelMaker for a persistent production outbox. */
    @Deprecated
    public ClientLocksHandler(Type type1) {
        this(type1, null);
    }

    public ClientLocksHandler(Type type, Path journalDirectory) {
        this.type = type;
        this.outbox = new MailJournal(journalDirectory);
    }

    public void lockManager(LockManager lockManager) {
        this.lockManager = BSUtils.faultIsolation(lockManager);
    }

    @Override
    public void init(ChannelRuntime runtime) {
        if (closing) throw new IllegalStateException("Mailbox is closed");
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
        eventLoop.repeat(this::flushPendingMails, 1_000, () -> closing);
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
                if (mailsA.contains(mail.uid()) || mailsB.contains(mail.uid())) {
                    r.response(mail, C2SMailResponsePacket.accepted(lock.token));
                    return;
                }
                if (lockManager.run(v -> v.acceptMail(mail.key(), mail.json(), mail.uid()))) {
                    mailsA.add(mail.uid());
                    r.response(mail, C2SMailResponsePacket.accepted(lock.token));
                } else {
                    log.error("Mail handler failed id={} owner={}; retaining for retry", mail.uid(), mail.key());
                    r.response(mail, C2SMailResponsePacket.reject(lock.token));
                }
            }
        } else {
            if (msg instanceof ChannelActiveMessage) {
                mailboxActive = true;
                flushPendingMails();
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
                // Requests in flight may already have committed. Retain their IDs and
                // retry after reconnect; a durable server receipt suppresses duplicates.
                inFlightMails.clear();
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
        if (eventLoop != null) eventLoop.assertThread();
        if (closing) return;
        closing = true;
        mailboxActive = false;
        inFlightMails.clear();
        outbox.close();
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
        if (lockManager != null) lockManager.run(LockManager::close);
        recovery.clear();
    }

    @Override
    public void pushMail(UUID key, String json) {
        if (!type.hasMailbox) throw new IllegalStateException("Mailbox is not enabled for this channel");
        if (closing) throw new IllegalStateException("Mailbox is closed");
        // Persist in the caller before returning, including when disconnected or before init.
        outbox.append(key, json);
        if (eventLoop != null) eventLoop.execute(this::flushPendingMails);
    }

    private void flushPendingMails() {
        if (closing || !mailboxActive) return;
        Set<UUID> owners = new HashSet<>();
        long now = System.nanoTime();
        for (Mail mail : outbox.pending()) {
            if (!owners.add(mail.owner())) continue; // preserve producer order per player
            if (inFlightMails.size() >= 32) return;
            if (inFlightMails.containsKey(mail.id()) || nextMailAttempt.getOrDefault(mail.id(), 0L) > now) continue;
            Object attempt = new Object();
            inFlightMails.put(mail.id(), attempt);
            new C2SPushMailPacket(mail.owner(), mail.payload(), mail.id())
                    .request(pipeline, remote, MAIL_REQUEST_TIMEOUT).then(response -> {
                        if (!inFlightMails.remove(mail.id(), attempt)) return;
                        if (closing) return; // the persisted record is replayed on reopening
                        if (response != null && response.flag()) {
                            try {
                                outbox.acknowledge(mail.id());
                                mailFailures.remove(mail.id());
                                nextMailAttempt.remove(mail.id());
                                eventLoop.schedule(this::flushPendingMails);
                            } catch (RuntimeException e) {
                                log.error("Cannot remove confirmed mail from outbox id={} owner={}", mail.id(), mail.owner(), e);
                            }
                        } else {
                            int attempts = mailFailures.merge(mail.id(), 1, Integer::sum);
                            long delay = Math.min(60_000L, 1_000L << Math.min(attempts, 6));
                            nextMailAttempt.put(mail.id(), System.nanoTime() + delay * 1_000_000L);
                            if (attempts == 1 || attempts % 10 == 0)
                                log.error("Mail not confirmed id={} owner={} attempts={} retryInMs={}; retained on disk",
                                        mail.id(), mail.owner(), attempts, delay);
                        }
                    });
        }
    }

    @Override
    public List<ExportSource> exportSources() {
        return exportSources(ShardSelection.ALL);
    }

    @Override
    public List<ExportSource> exportSources(ShardSelection shard) {
        Objects.requireNonNull(shard);
        return List.of(new ExportSource() {
            private UUID afterKey;
            private long afterSequence;

            @Override
            public CompletableFuture<S2CExportPagePacket> next(
                TransferEntry.Kind kind) {
                if ((kind == TransferEntry.Kind.DATA && !type.hasBlobs)
                        || (kind == TransferEntry.Kind.MAIL && !type.hasMailbox))
                    return CompletableFuture.completedFuture(S2CExportPagePacket.empty());
                var result = new CompletableFuture<S2CExportPagePacket>();
                if (eventLoop == null || closing) return CompletableFuture.failedFuture(new IllegalStateException("Channel not initialized"));
                eventLoop.executeReliably(() -> {
                    if (closing || !mailboxActive) {
                        result.completeExceptionally(new IllegalStateException("Channel is disconnected"));
                        return;
                    }
                    new C2SExportPagePacket(kind, afterKey, afterSequence, shard.count(), shard.index()).request(pipeline, remote, 30_000).then(page -> {
                        if (page == null) {
                            result.completeExceptionally(new TimeoutException("Export page was not confirmed"));
                        } else if (!page.error().isEmpty()) {
                            result.completeExceptionally(new IllegalStateException(page.error()));
                        } else {
                            afterKey = page.afterKey();
                            afterSequence = page.afterSequence();
                            result.complete(page);
                        }
                    });
                });
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<TransferResult> importEntry(
        TransferEntry entry) {
        var result = new CompletableFuture<TransferResult>();
        if (eventLoop == null || closing) return CompletableFuture.failedFuture(new IllegalStateException("Channel not initialized"));
        eventLoop.executeReliably(() -> {
            if (closing || !mailboxActive) {
                result.completeExceptionally(new IllegalStateException("Channel is disconnected"));
            } else if (entry.kind() == TransferEntry.Kind.DATA) {
                if (!type.hasBlobs) {
                    result.complete(new TransferResult(
                            TransferResult.Status.UNSUPPORTED, "Channel has no blob storage"));
                    return;
                }
                new C2SImportDataPacket(entry.key(), entry.payload()).request(pipeline, remote, 30_000).then(response -> {
                    if (response == null) result.completeExceptionally(new TimeoutException("No snapshot import response"));
                    else result.complete(new TransferResult(response.status(), response.detail()));
                });
            } else {
                if (!type.hasMailbox) {
                    result.complete(new TransferResult(
                            TransferResult.Status.UNSUPPORTED, "Channel has no mailbox"));
                    return;
                }
                String payload = new String(entry.payload(), StandardCharsets.UTF_8);
                new C2SPushMailPacket(entry.key(), payload, entry.messageId()).request(pipeline, remote, 30_000).then(response -> {
                    if (response == null) result.completeExceptionally(new TimeoutException("No mail import response"));
                    else result.complete(new TransferResult(response.flag()
                            ? TransferResult.Status.SUCCESS
                            : TransferResult.Status.FAILED, response.flag() ? "" : "Mail SQL admission failed"));
                });
            }
        });
        return result;
    }

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
