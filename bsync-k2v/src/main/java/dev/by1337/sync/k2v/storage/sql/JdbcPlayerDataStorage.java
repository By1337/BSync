package dev.by1337.sync.k2v.storage.sql;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.common.mail.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.common.transfer.*;
import dev.by1337.sync.k2v.storage.PlayerDataStorage;
import org.slf4j.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Direct SQL transport. No JDBC call runs on the calling/Bukkit thread. */
public final class JdbcPlayerDataStorage implements PlayerDataStorage {
    private static final Logger log = LoggerFactory.getLogger(JdbcPlayerDataStorage.class);
    private final SqlStorageConfig config;
    private final SqlLeaseRepository database;
    private final List<HikariDataSource> pools;
    private final boolean blobs, mailbox;
    private final MailJournal outbox;
    private final Path recoveryDirectory;
    private final LongSupplier clock;
    private final ThreadPoolExecutor io;
    private final ExecutorService renewIo;
    private final ScheduledExecutorService timer;
    private final ConcurrentMap<UUID, Lease> leases = new ConcurrentHashMap<>();
    private final AtomicInteger versions = new AtomicInteger();
    private final AtomicBoolean initRunning = new AtomicBoolean(), mailRunning = new AtomicBoolean(), outboxRunning = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> initialized = new CompletableFuture<>();
    private volatile boolean ready;
    private volatile long initAfter, mailAfter, outboxAfter;
    private int initFailures, outboxFailures;
    private volatile Consumer<Mail> mailHandler;
    private volatile BiConsumer<UUID, Integer> lockLost = (key, version) -> {
    };
    private volatile O2BTester<UUID> validator = key -> true;

    public JdbcPlayerDataStorage(SqlStorageConfig config, Path directory, boolean blobs, boolean mailbox) {
        this(config, resources(config), directory, blobs, mailbox);
    }

    private JdbcPlayerDataStorage(SqlStorageConfig config, Resources resources, Path directory, boolean blobs, boolean mailbox) {
        this(config, new SqlLeaseRepository(resources.data, resources.heartbeat, config), openJournal(resources, directory, mailbox),
                directory.resolve("lost-snapshots"), blobs, mailbox, System::nanoTime,
                resources.data == resources.heartbeat ? List.of(resources.data) : List.of(resources.data, resources.heartbeat));
    }

    JdbcPlayerDataStorage(SqlStorageConfig config, SqlLeaseRepository database, MailJournal outbox, Path recovery,
        boolean blobs, boolean mailbox, LongSupplier clock) {
        this(config, database, outbox, recovery, blobs, mailbox, clock, List.of());
    }

    private JdbcPlayerDataStorage(SqlStorageConfig config, SqlLeaseRepository database, MailJournal outbox, Path recovery,
        boolean blobs, boolean mailbox, LongSupplier clock, List<HikariDataSource> pools) {
        this.config = config;
        this.database = database;
        this.outbox = outbox;
        this.recoveryDirectory = recovery;
        this.blobs = blobs;
        this.mailbox = mailbox;
        this.clock = clock;
        this.pools = pools;
        io = new ThreadPoolExecutor(config.poolSize, config.poolSize, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(2048), task -> daemon(task, "bsync-sql-io"), new ThreadPoolExecutor.AbortPolicy());
        renewIo = Executors.newSingleThreadExecutor(task -> daemon(task, "bsync-sql-lease"));
        timer = Executors.newSingleThreadScheduledExecutor(task -> daemon(task, "bsync-sql-watchdog"));
        timer.scheduleWithFixedDelay(() -> {
            try {
                maintenance();
            } catch (Exception e) {
                log.error("SQL storage maintenance failed for {}", config, e);
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private static Resources resources(SqlStorageConfig config) {
        HikariDataSource data = SqlDataSources.create(config, config.poolSize, "bsync-sql-data");
        if (config.isSqlite()) return new Resources(data, data);
        try {
            return new Resources(data, SqlDataSources.create(config, 1, "bsync-sql-lease"));
        } catch (RuntimeException e) {
            data.close();
            throw e;
        }
    }

    private static MailJournal openJournal(Resources resources, Path directory, boolean enabled) {
        try {
            return new MailJournal(enabled ? directory.resolve("mail-outbox") : null);
        } catch (RuntimeException e) {
            resources.data.close();
            resources.heartbeat.close();
            throw e;
        }
    }

    private static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    private long now() {
        return clock.getAsLong();
    }

    private static long nanos(long millis) {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    private boolean alive(Lease lease) {
        return lease.owned && (config.isSqlite() || now() - lease.deadline < 0);
    }

    CompletableFuture<Void> initialized() {
        return initialized;
    }

    void maintenance() {
        long now = now();
        if (!ready && !closing.get() && now - initAfter >= 0 && initRunning.compareAndSet(false, true)) {
            if (!submit(() -> {
                try {
                    database.initialize(mailbox);
                    ready = true;
                    initialized.complete(null);
                    if (config.isSqlite()) log.info("Local SQLite storage initialized: {}", config);
                    else log.info("Direct SQL storage initialized: {} lease={}ms renew={}ms", config, config.leaseMillis, config.renewMillis);
                } catch (Exception e) {
                    initAfter = now() + nanos(backoff(++initFailures));
                    log.error("SQL storage initialization failed for {}; retrying", config, e);
                } finally {
                    initRunning.set(false);
                }
            })) initRunning.set(false);
        }
        for (Lease lease : leases.values()) {
            if (!lease.owned) continue;
            if (!config.isSqlite() && now - lease.deadline >= 0) {
                lost(lease, "Local lease deadline expired");
                continue;
            }
            if (!lease.releasing && !validator.test(lease.key)) {
                unlock(lease.key, lease.version);
            }
            if (!config.isSqlite() && now - lease.renewAt >= 0 && lease.renewing.compareAndSet(false, true)) {
                renewIo.execute(() -> renew(lease));
            }
            if ((lease.releasing || lease.revision > lease.savedRevision) && now - lease.writeAfter >= 0) queueWrite(lease);
        }
        if (!closing.get() && ready) {
            if (mailbox && now - outboxAfter >= 0 && !outbox.pending().isEmpty()) flushOutbox();
            if (mailbox && now - mailAfter >= 0) pollMails();
        }
    }

    @Override
    public int lockAndLoadData(UUID key, BiConsumer<Boolean, byte[]> callback) {
        int version = versions.updateAndGet(value -> value == Integer.MAX_VALUE ? 1 : value + 1);
        Lease lease = new Lease(key, version, callback, now() + nanos(config.acquireWaitMillis));
        if (closing.get() || leases.putIfAbsent(key, lease) != null) {
            callback.accept(false, null);
            return version;
        }
        acquire(lease);
        return version;
    }

    private void acquire(Lease lease) {
        if (!submit(() -> {
            if (closing.get() || leases.get(lease.key) != lease || lease.releasing) {
                completeLoad(lease, false, null);
                return;
            }
            long started = now();
            try {
                SqlLeaseRepository.Claim claim = ready ? database.claim(lease.key, lease.token) : null;
                if (claim == null) {
                    retryAcquire(lease);
                    return;
                }
                boolean accepted;
                synchronized (lease) {
                    lease.epoch = claim.epoch();
                    lease.deadline = started + nanos(claim.remainingMillis());
                    lease.renewAt = lease.deadline - nanos(config.leaseMillis - config.renewMillis);
                    accepted = !closing.get() && !lease.releasing && leases.get(lease.key) == lease
                            && (config.isSqlite() || now() - lease.deadline < 0);
                    lease.owned = accepted;
                }
                if (!accepted) {
                    database.save(lease.key, lease.token, lease.epoch, null, true);
                    leases.remove(lease.key, lease);
                    completeLoad(lease, false, null);
                    return;
                }
                completeLoad(lease, true, blobs ? claim.data() : null);
            } catch (Exception e) {
                log.warn("SQL acquire failed for {}; retrying within acquisition deadline", lease.key, e);
                retryAcquire(lease);
            }
        })) retryAcquire(lease);
    }

    private void retryAcquire(Lease lease) {
        if (closing.get() || lease.releasing || leases.get(lease.key) != lease || now() - lease.acquireUntil >= 0) {
            leases.remove(lease.key, lease);
            completeLoad(lease, false, null);
        } else timer.schedule(() -> acquire(lease), 500, TimeUnit.MILLISECONDS);
    }

    private void completeLoad(Lease lease, boolean success, byte[] snapshot) {
        if (lease.loaded.compareAndSet(false, true)) lease.callback.accept(success, snapshot);
    }

    private void renew(Lease lease) {
        long started = now();
        try {
            if (!lease.owned) return;
            if (!database.renew(lease.key, lease.token, lease.epoch)) {
                // The final save may have cleared ownership concurrently. Its result
                // decides whether release succeeded; the watchdog still bounds failures.
                if (lease.releasing && lease.writing.get()) return;
                lost(lease, "Database rejected lease renewal");
                return;
            }
            synchronized (lease) {
                if (!lease.owned || leases.get(lease.key) != lease || now() - lease.deadline >= 0) {
                    // A late successful response cannot revive locally invalidated ownership.
                    lostAfterRenewal(lease);
                    return;
                }
                lease.deadline = started + nanos(config.leaseMillis);
                lease.renewAt = started + nanos(config.renewMillis);
            }
        } catch (Exception e) {
            lease.renewAt = now() + nanos(500);
            log.warn("SQL lease renewal unavailable for {}; local deadline is not extended", lease.key, e);
        } finally {
            lease.renewing.set(false);
        }
    }

    private void lostAfterRenewal(Lease lease) {
        // Notification is queued so no callback acquires repository locks under the lease monitor.
        timer.execute(() -> lost(lease, "Renewal completed after the local deadline"));
        submit(() -> {
            try {
                database.save(lease.key, lease.token, lease.epoch, null, true);
            } catch (Exception e) {
                log.warn("Cannot release late SQL lease for {}", lease.key, e);
            }
        });
    }

    @Override
    public void pushSnapshot(UUID key, byte[] snapshot) {
        if (snapshot == null) return;
        if (!blobs) throw new IllegalStateException("This repository has no blob storage");
        if (snapshot.length > TransferEntry.MAX_DATA_BYTES) throw new IllegalArgumentException("Snapshot is too large");
        Lease lease = leases.get(key);
        if (lease == null) throw new IllegalStateException("Player has no SQL lease: " + key);
        synchronized (lease) {
            if (!alive(lease) || lease.releasing) throw new IllegalStateException("Player SQL lease is not writable: " + key);
            if (lease.revision == lease.savedRevision) lease.writeAfter = now() + nanos(50);
            lease.snapshot = snapshot.clone();
            lease.revision++;
        }
    }

    private void queueWrite(Lease lease) {
        if (!lease.writing.compareAndSet(false, true)) return;
        if (!submit(() -> {
            byte[] snapshot;
            long revision;
            boolean release;
            synchronized (lease) {
                snapshot = lease.snapshot;
                revision = lease.revision;
                release = lease.releasing;
            }
            try {
                if (!alive(lease)) {
                    lost(lease, "Lease expired before snapshot commit");
                    return;
                }
                if (!database.save(lease.key, lease.token, lease.epoch, snapshot, release)) {
                    lost(lease, "Database rejected fenced snapshot write");
                    return;
                }
                lease.savedRevision = revision;
                lease.writeFailures = 0;
                if (release) {
                    lease.owned = false;
                    leases.remove(lease.key, lease);
                }
            } catch (Exception e) {
                lease.writeAfter = now() + nanos(backoff(++lease.writeFailures));
                log.error("SQL snapshot commit failed for {}; retained until the lease deadline", lease.key, e);
            } finally {
                lease.writing.set(false);
            }
        })) lease.writing.set(false);
    }

    @Override
    public void unlock(UUID key, int version) {
        Lease lease = leases.get(key);
        if (lease == null || (version != -1 && version != lease.version)) return;
        synchronized (lease) {
            lease.releasing = true;
            lease.writeAfter = now();
        }
        if (!lease.owned) {
            leases.remove(key, lease);
            completeLoad(lease, false, null);
        } else queueWrite(lease);
    }

    private void lost(Lease lease, String reason) {
        byte[] snapshot;
        synchronized (lease) {
            if (!lease.owned) return;
            lease.owned = false;
            snapshot = lease.revision > lease.savedRevision ? lease.snapshot : null;
            leases.remove(lease.key, lease);
        }
        log.error("SQL lease lost player={} token={} epoch={} reason={}; stale writes are fenced", lease.key, lease.token, lease.epoch, reason);
        if (snapshot != null) quarantine(lease, snapshot);
        try {
            lockLost.accept(lease.key, lease.version);
        } catch (Exception e) {
            log.error("SQL lock-loss callback failed for {}", lease.key, e);
        }
    }

    private void quarantine(Lease lease, byte[] snapshot) {
        if (!submit(() -> {
            try {
                Files.createDirectories(recoveryDirectory);
                Path target = recoveryDirectory.resolve(lease.key + "-" + lease.token + ".dat");
                Files.write(target, snapshot, StandardOpenOption.CREATE_NEW);
                log.error("Unsaved fenced snapshot retained at {}; it will not be replayed over a newer owner", target);
            } catch (Exception e) {
                log.error("Cannot retain fenced snapshot for {}", lease.key, e);
            }
        })) log.error("Cannot queue recovery snapshot for {}; SQL IO queue is full or closed", lease.key);
    }

    @Override
    public boolean isLocked(UUID key) {
        Lease lease = leases.get(key);
        return lease != null && alive(lease) && !lease.releasing;
    }

    @Override
    public CompletableFuture<byte[]> loadSnapshot(UUID key) {
        return supply(() -> {
            requireReady();
            return database.snapshot(key);
        });
    }

    @Override
    public void setLockValidator(O2BTester<UUID> validator) {
        this.validator = validator;
    }

    @Override
    public void setLockLostHandler(BiConsumer<UUID, Integer> handler) {
        lockLost = Objects.requireNonNull(handler);
    }

    @Override
    public void setMailAccept(BiConsumer<UUID, String> handler) {
        mailHandler = mail -> handler.accept(mail.owner(), mail.payload());
    }

    @Override
    public void setMailHandler(Consumer<Mail> handler) {
        mailHandler = handler;
    }

    @Override
    public void pushMail(UUID key, String payload) {
        if (!mailbox) throw new IllegalStateException("This repository has no mailbox");
        if (closing.get()) throw new IllegalStateException("Storage is closed");
        outbox.append(key, payload);
        outboxAfter = now();
    }

    private void flushOutbox() {
        if (!outboxRunning.compareAndSet(false, true)) return;
        if (!submit(() -> {
            try {
                int limit = 32;
                for (Mail mail : outbox.pending()) {
                    if (limit-- == 0) break;
                    if (!database.admit(mail)) throw new IllegalStateException("Conflicting mailbox ID " + mail.id());
                    outbox.acknowledge(mail.id());
                    outboxFailures = 0;
                    Lease lease = leases.get(mail.owner());
                    if (lease != null && lease.mailReady) mailAfter = now();
                }
                outboxAfter = now();
            } catch (Exception e) {
                outboxAfter = now() + nanos(backoff(++outboxFailures));
                log.error("SQL mail admission failed for {}; persisted outbox will retry", config, e);
            } finally {
                outboxRunning.set(false);
            }
        })) outboxRunning.set(false);
    }

    @Override
    public void doMailsLoad(UUID key) {
        Lease lease = leases.get(key);
        if (lease != null) {
            lease.mailReady = true;
            mailAfter = now();
        }
    }

    private void pollMails() {
        if (mailHandler == null) return;
        List<Lease> owned = leases.values().stream().filter(lease -> alive(lease) && lease.mailReady && !lease.releasing).toList();
        mailAfter = now() + nanos(config.pollMillis);
        if (owned.isEmpty() || !mailRunning.compareAndSet(false, true)) return;
        if (!submit(() -> {
            try {
                for (int offset = 0; offset < owned.size(); offset += 256) {
                    List<UUID> tokens = owned.subList(offset, Math.min(offset + 256, owned.size())).stream().map(lease -> lease.token).toList();
                    for (var pending : database.pending(tokens)) {
                        Lease lease = leases.get(pending.mail().owner());
                        if (lease == null || !alive(lease) || lease.releasing || !lease.token.equals(pending.token())) continue;
                        try {
                            mailHandler.accept(pending.mail());
                            if (!database.acknowledge(pending)) throw new IllegalStateException("Fenced mail ACK was not accepted");
                            mailAfter = now();
                        } catch (Exception e) {
                            long delay = backoff((int) Math.min(Integer.MAX_VALUE, pending.attempts() + 1));
                            log.error("SQL mail handler failed id={} player={} attempts={} retryMs={}",
                                    pending.mail().id(), pending.mail().owner(), pending.attempts() + 1, delay, e);
                            database.failed(pending, e.toString(), delay);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("SQL mailbox poll failed for {}; messages remain pending", config, e);
            } finally {
                mailRunning.set(false);
            }
        })) mailRunning.set(false);
    }

    @Override
    public List<ExportSource> exportSources() {
        return List.of(new ExportSource() {
            UUID afterKey;
            long sequence;

            @Override
            public CompletableFuture<S2CExportPagePacket> next(TransferEntry.Kind kind) {
                return supply(() -> {
                    requireReady();
                    if ((kind == TransferEntry.Kind.DATA && !blobs) || (kind == TransferEntry.Kind.MAIL && !mailbox)) return S2CExportPagePacket.empty();
                    S2CExportPagePacket page = kind == TransferEntry.Kind.DATA ? database.exportData(afterKey) : database.exportMails(sequence);
                    afterKey = page.afterKey();
                    sequence = page.afterSequence();
                    return page;
                });
            }
        });
    }

    @Override
    public CompletableFuture<TransferResult> importEntry(TransferEntry entry) {
        return supply(() -> {
            requireReady();
            if (entry.kind() == TransferEntry.Kind.MAIL) {
                if (!mailbox) return new TransferResult(TransferResult.Status.UNSUPPORTED, "Mailbox is disabled");
                boolean accepted = database.admit(new Mail(entry.messageId(), entry.key(), new String(entry.payload(), StandardCharsets.UTF_8)));
                if (accepted) mailAfter = now();
                return new TransferResult(accepted ? TransferResult.Status.SUCCESS : TransferResult.Status.FAILED, accepted ? "" : "Conflicting mail ID");
            }
            if (!blobs) return new TransferResult(TransferResult.Status.UNSUPPORTED, "Blob storage is disabled");
            if (leases.containsKey(entry.key())) return new TransferResult(TransferResult.Status.LOCKED, "Player is locally leased");
            UUID token = UUID.randomUUID();
            SqlLeaseRepository.Claim claim = database.claim(entry.key(), token);
            if (claim == null) return new TransferResult(TransferResult.Status.LOCKED, "Player is leased in SQL");
            boolean released = false;
            try {
                released = database.save(entry.key(), token, claim.epoch(), entry.payload(), true);
                return released ? TransferResult.success()
                        : new TransferResult(TransferResult.Status.FAILED, "Import lease expired");
            } finally {
                if (!released) database.save(entry.key(), token, claim.epoch(), null, true);
            }
        });
    }

    private void requireReady() {
        if (closing.get() || !ready) throw new IllegalStateException("SQL storage is closed or not initialized");
    }

    private boolean submit(Runnable task) {
        try {
            io.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    private <T> CompletableFuture<T> supply(SqlTask<T> task) {
        var future = new CompletableFuture<T>();
        if (closing.get() || !submit(() -> {
            try {
                future.complete(task.run());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        })) future.completeExceptionally(new RejectedExecutionException("SQL storage is closed or busy"));
        return future;
    }

    private static long backoff(int failures) {
        return Math.min(60_000, 500L << Math.min(Math.max(failures, 1), 7));
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) return;
        for (Lease lease : leases.values()) unlock(lease.key, lease.version);
        long until = System.nanoTime() + nanos(Math.min(10_000, config.timeoutMillis * 4 + 1_000));
        while (!leases.isEmpty() && System.nanoTime() - until < 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        for (Lease lease : leases.values()) lost(lease, "Storage shutdown before final commit");
        timer.shutdownNow();
        renewIo.shutdownNow();
        io.shutdown();
        try {
            if (!io.awaitTermination(config.timeoutMillis + 500, TimeUnit.MILLISECONDS)) io.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            io.shutdownNow();
        }
        outbox.close();
        pools.forEach(HikariDataSource::close);
        try {
            database.close();
        } catch (Exception e) {
            log.error("Cannot close SQL repository ownership", e);
        }
        initialized.completeExceptionally(new IllegalStateException("Storage closed before initialization"));
    }

    private record Resources(HikariDataSource data, HikariDataSource heartbeat) {
    }

    @FunctionalInterface
    private interface SqlTask<T> {
        T run() throws Exception;
    }

    private static final class Lease {
        final UUID key, token = UUID.randomUUID();
        final int version;
        final BiConsumer<Boolean, byte[]> callback;
        final long acquireUntil;
        final AtomicBoolean loaded = new AtomicBoolean(), writing = new AtomicBoolean(), renewing = new AtomicBoolean();
        volatile boolean owned, releasing, mailReady;
        volatile long epoch, deadline, renewAt, revision, savedRevision, writeAfter;
        volatile byte[] snapshot;
        int writeFailures;

        Lease(UUID key, int version, BiConsumer<Boolean, byte[]> callback, long acquireUntil) {
            this.key = key;
            this.version = version;
            this.callback = callback;
            this.acquireUntil = acquireUntil;
        }
    }
}
