package dev.by1337.sync.k2v.storage;

import dev.by1337.sync.k2v.mail.Mail;
import dev.by1337.sync.k2v.mail.MailJournal;
import dev.by1337.sync.k2v.transfer.ExportPage;
import dev.by1337.sync.k2v.transfer.ExportSource;
import dev.by1337.sync.k2v.transfer.TransferEntry;
import dev.by1337.sync.k2v.transfer.TransferResult;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class FilePlayerDataStorage implements PlayerDataStorage {
    private static final Logger log = LoggerFactory.getLogger(FilePlayerDataStorage.class);
    private final Object[] locks;
    private final Path dataFolder;
    private volatile java.util.function.Consumer<Mail> mailAccept;
    private volatile boolean closed;
    private final MailJournal mailbox;
    private final Path receipts;
    private static final ExecutorService IMPORT_IO = Executors.newFixedThreadPool(4, task -> {
        Thread thread = new Thread(task, "bsync-local-import");
        thread.setDaemon(true);
        return thread;
    });
    private volatile O2BTester<UUID> validator = key -> false;
    private final ScheduledExecutorService mailWorker;
    private final java.util.Map<UUID, Integer> failures = new java.util.HashMap<>();

    public FilePlayerDataStorage(File dataFolder) {
        dataFolder.mkdirs();
        this.dataFolder = dataFolder.toPath();
        locks = new Object[512];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
        mailbox = new MailJournal(this.dataFolder.resolve("mailbox"));
        receipts = this.dataFolder.resolve("mail-receipts");
        try {
            Files.createDirectories(receipts);
        } catch (IOException e) {
            mailbox.close();
            throw new UncheckedIOException(e);
        }
        migrateLegacyMails();
        mailWorker = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "bsync-local-mailbox");
            thread.setDaemon(true);
            return thread;
        });
        mailWorker.scheduleWithFixedDelay(() -> {
            try {
                for (UUID owner : mailbox.pending().stream().map(Mail::owner).distinct().toList()) {
                    if (validator.test(owner)) deliverMails(owner);
                }
            } catch (Exception e) {
                log.error("Local mailbox retry failed", e);
            }
        }, 1, 2, TimeUnit.SECONDS);
    }

    public void write(UUID key, byte @Nullable [] data) {
        if (data == null) return;
        var tmp = dataFolder.resolve(key + ".dat.tmp");
        try {
            synchronized (route(key)) {
                Files.write(tmp, data);
                Files.move(tmp, dataFolder.resolve(key + ".dat"), StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (IOException e) {
            log.error("Failed to write data to {}", dataFolder.resolve(key + ".dat"), e);
        }
        try {
            Files.deleteIfExists(tmp);
        } catch (IOException ignored) {
        }
    }

    public byte @Nullable [] read(UUID key) {
        try {
            synchronized (route(key)) {
                var path = dataFolder.resolve(key + ".dat");
                if (Files.exists(path)) {
                    return Files.readAllBytes(path);
                }
            }
        } catch (IOException e) {
            log.error("Failed to read data {}", dataFolder.resolve(key + ".dat"), e);
        }
        return null;
    }

    public void appendMail(UUID key, String mail) {
        mailbox.append(key, mail);
    }

    /** Explicit destructive drain retained for compatibility; delivery never uses it. */
    @Deprecated
    public List<String> readAllMailsAndDelete(UUID key) {
        synchronized (route(key)) {
            var mails = mailbox.pending().stream().filter(mail -> mail.owner().equals(key)).toList();
            for (Mail mail : mails) mailbox.acknowledge(mail.id());
            return mails.stream().map(Mail::payload).toList();
        }
    }

    private void migrateLegacyMails() {
        try (var paths = Files.list(dataFolder)) {
            for (Path path : paths.filter(p -> p.toString().endsWith(".mailbox")).toList()) {
                String filename = path.getFileName().toString();
                UUID owner = UUID.fromString(filename.substring(0, filename.length() - ".mailbox".length()));
                var lines = Files.readAllLines(path, StandardCharsets.UTF_8);
                for (int i = 0; i < lines.size(); i++) {
                    UUID id = UUID.nameUUIDFromBytes((filename + ":" + i).getBytes(StandardCharsets.UTF_8));
                    mailbox.append(new Mail(id, owner, lines.get(i).replace("\\n", "\n")));
                }
                Files.delete(path);
            }
        } catch (IOException | RuntimeException e) {
            mailbox.close();
            throw new IllegalStateException("Cannot migrate legacy local mailbox", e);
        }
    }

    private void deliverMails(UUID key) {
        synchronized (route(key)) {
            for (Mail mail : mailbox.pending()) {
                if (!mail.owner().equals(key)) continue;
                if (closed || !validator.test(key) || mailAccept == null) return;
                try {
                    if (!hasReceipt(mail)) {
                        mailAccept.accept(mail);
                        if (closed) return;
                        writeReceipt(mail);
                    }
                    mailbox.acknowledge(mail.id());
                    failures.remove(mail.id());
                } catch (Exception e) {
                    int attempts = failures.merge(mail.id(), 1, Integer::sum);
                    if (attempts == 1 || attempts % 10 == 0)
                        log.error("Local mail delivery failed id={} owner={} attempts={}; retained for retry",
                                mail.id(), key, attempts, e);
                    return; // preserve per-player order and retry, without deleting the mail
                }
            }
        }
    }

    private boolean hasReceipt(Mail mail) throws IOException {
        Path path = receipts.resolve(mail.id() + ".receipt");
        if (!Files.exists(path)) return false;
        if (Files.size(path) != 32) throw new IOException("Invalid mail receipt " + path);
        if (!MessageDigest.isEqual(Files.readAllBytes(path), fingerprint(mail)))
            throw new IOException("Conflicting delivered mail ID " + mail.id());
        return true;
    }

    private static byte[] fingerprint(Mail mail) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(16).putLong(mail.owner().getMostSignificantBits())
                    .putLong(mail.owner().getLeastSignificantBits()).array());
            return digest.digest(mail.payload().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private void writeReceipt(Mail mail) throws IOException {
        atomicWrite(receipts.resolve(mail.id() + ".receipt"), fingerprint(mail));
    }

    private static void atomicWrite(Path target, byte[] payload) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                var buffer = ByteBuffer.wrap(payload);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (var directory = FileChannel.open(target.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    @Override
    public CompletableFuture<TransferResult> importEntry(
        TransferEntry entry) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Storage is closed"));
        return CompletableFuture.supplyAsync(() -> {
            synchronized (route(entry.key())) {
                if (closed) throw new IllegalStateException("Storage is closed");
                if (entry.kind() == TransferEntry.Kind.DATA && validator.test(entry.key()))
                    return new TransferResult(
                            TransferResult.Status.LOCKED, "Player is loaded locally");
                try {
                    if (entry.kind() == TransferEntry.Kind.DATA) {
                        atomicWrite(dataFolder.resolve(entry.key() + ".dat"), entry.payload());
                    } else {
                        var mail = new Mail(entry.messageId(), entry.key(), new String(entry.payload(), StandardCharsets.UTF_8));
                        if (!hasReceipt(mail)) mailbox.append(mail);
                    }
                    return TransferResult.success();
                } catch (Exception e) {
                    log.error("Local import failed kind={} key={}", entry.kind(), entry.key(), e);
                    return new TransferResult(
                            TransferResult.Status.FAILED, e.toString());
                }
            }
        }, IMPORT_IO);
    }

    @Override
    public List<ExportSource> exportSources() {
        return List.of(new ExportSource() {
            private java.nio.file.DirectoryStream<Path> dataFiles;
            private Iterator<Path> data;
            private Iterator<Mail> mails;
            private TransferEntry carry;

            @Override
            public CompletableFuture<ExportPage> next(
                TransferEntry.Kind kind) {
                try {
                    if (closed) throw new IllegalStateException("Storage is closed");
                    if (kind == TransferEntry.Kind.DATA && data == null) {
                        dataFiles = Files.newDirectoryStream(dataFolder, "*.dat");
                        data = dataFiles.iterator();
                    }
                    if (kind == TransferEntry.Kind.MAIL && mails == null)
                        mails = mailbox.pending().iterator();
                    var entries = new ArrayList<TransferEntry>();
                    int bytes = 0;
                    while (entries.size() < TransferEntry.PAGE_ROWS) {
                        TransferEntry entry;
                        if (carry != null) {
                            entry = carry;
                            carry = null;
                        } else if (kind == TransferEntry.Kind.DATA) {
                            if (!data.hasNext()) break;
                            Path path = data.next();
                            String name = path.getFileName().toString();
                            UUID key = UUID.fromString(name.substring(0, name.length() - 4));
                            synchronized (route(key)) {
                                if (Files.size(path) > TransferEntry.MAX_DATA_BYTES)
                                    throw new IOException("Oversized snapshot " + path);
                                entry = new TransferEntry(kind, key, Files.readAllBytes(path), null);
                            }
                        } else {
                            if (!mails.hasNext()) break;
                            Mail mail = mails.next();
                            entry = new TransferEntry(kind, mail.owner(),
                                    mail.payload().getBytes(StandardCharsets.UTF_8), mail.id());
                        }
                        if (!entries.isEmpty() && bytes + entry.payload().length > TransferEntry.PAGE_BYTES) {
                            carry = entry;
                            break;
                        }
                        entries.add(entry);
                        bytes += entry.payload().length;
                        if (bytes >= TransferEntry.PAGE_BYTES) break;
                    }
                    boolean done = carry == null && !(kind == TransferEntry.Kind.DATA ? data.hasNext() : mails.hasNext());
                    return CompletableFuture.completedFuture(new ExportPage(
                            List.copyOf(entries), null, 0, done, ""));
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }

            @Override
            public void close() throws IOException {
                if (dataFiles != null) dataFiles.close();
            }
        });
    }

    private Object route(UUID uuid) {
        return locks[Math.floorMod(hash(uuid), locks.length)];
    }

    static long hash(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }

    @Override
    public void setLockValidator(O2BTester<UUID> tester) {
        validator = tester;
    }

    @Override
    public void doMailsLoad(UUID key) {
        mailWorker.execute(() -> deliverMails(key));
    }

    @Override
    public void setMailAccept(BiConsumer<UUID, String> accept) {
        mailAccept = mail -> accept.accept(mail.owner(), mail.payload());
    }

    @Override
    public void setMailHandler(java.util.function.Consumer<Mail> accept) {
        mailAccept = accept;
    }

    @Override
    @Deprecated
    public boolean isLocked(UUID uuid) {
        return true;
    }

    @Override
    public void pushMail(UUID key, String json) {
        appendMail(key, json);
    }

    @Override
    public void pushSnapshot(UUID key, byte @Nullable [] snapshot) {
        write(key, snapshot);
    }

    @Override
    public CompletableFuture<byte @Nullable []> loadSnapshot(UUID key) {
        return CompletableFuture.completedFuture(read(key));
    }

    @Override
    @Deprecated
    public void unlock(UUID key, int version) {
    }

    @Override
    public int lockAndLoadData(UUID key, BiConsumer<Boolean, byte @Nullable []> callback) {
        callback.accept(true, read(key));
        return 1;
    }

    @Override
    public void close() {
        closed = true;
        mailWorker.shutdownNow();
        mailbox.close();
    }
}
