package dev.by1337.sync.k2v.transfer;

import dev.by1337.sync.k2v.transfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

public final class RepositoryTransfer {
    private static final Logger log = LoggerFactory.getLogger(RepositoryTransfer.class);
    private static final int BUFFER = 64 << 10;
    private static final int WINDOW_BYTES = 16 << 20;
    private static final int MAX_LINE = (TransferEntry.MAX_DATA_BYTES + 2) / 3 * 4 + 128;
    private static final ExecutorService FILES = Executors.newFixedThreadPool(2, task -> daemon(task, "bsync-transfer-file"));
    private static final ScheduledExecutorService RETRIES = Executors.newSingleThreadScheduledExecutor(task -> daemon(task, "bsync-transfer-retry"));
    private static final ExecutorService PARALLEL = Executors.newSingleThreadExecutor(task -> daemon(task, "bsync-transfer-parallel"));

    private RepositoryTransfer() {
    }

    private static Thread daemon(Runnable task, String name) {
        Thread t = new Thread(task, name);
        t.setDaemon(true);
        return t;
    }

    public static CompletableFuture<TransferStats> exportTo(TransferStorage storage, Path destination) {
        return CompletableFuture.supplyAsync(() -> {
            Path target = destination.toAbsolutePath();
            Path temporary = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
            List<ExportSource> sources = List.of();
            long data = 0, mails = 0;
            try {
                Files.createDirectories(target.getParent());
                sources = storage.exportSources();
                try (var writer = new BufferedWriter(Files.newBufferedWriter(temporary, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), BUFFER)) {
                    writer.write("#bsync-transfer 1\n");
                    for (TransferEntry.Kind kind : TransferEntry.Kind.values()) {
                        writer.write(kind == TransferEntry.Kind.DATA ? "#data\n" : "#mails\n");
                        for (ExportSource source : sources) {
                            UUID previousKey = null;
                            long previousSequence = 0;
                            while (true) {
                                var page = source.next(kind).get();
                                if (!page.error().isEmpty()) throw new IOException(page.error());
                                if (!page.done() && page.entries().isEmpty()
                                        && Objects.equals(page.afterKey(), previousKey) && page.afterSequence() == previousSequence)
                                    throw new IOException("Export source did not advance its cursor");
                                previousKey = page.afterKey();
                                previousSequence = page.afterSequence();
                                for (TransferEntry entry : page.entries()) {
                                    if (entry.kind() != kind) throw new IOException("Unexpected export section");
                                    writer.write(entry.key().toString());
                                    writer.write(' ');
                                    writer.write(Base64.getEncoder().encodeToString(entry.payload()));
                                    if (kind == TransferEntry.Kind.MAIL) {
                                        writer.write(' ');
                                        writer.write(entry.messageId().toString());
                                        mails++;
                                    } else data++;
                                    writer.newLine();
                                }
                                if (page.done()) break;
                            }
                        }
                    }
                }
                try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                log.info("Repository export complete path={} data={} mails={}", target, data, mails);
                return new TransferStats(data, mails, 0, 0);
            } catch (Exception e) {
                throw new CompletionException("Export failed: " + target, e);
            } finally {
                for (ExportSource source : sources) {
                    try {
                        source.close();
                    } catch (Exception e) {
                        log.error("Failed to close export source", e);
                    }
                }
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException e) {
                    log.warn("Cannot remove incomplete export {}", temporary, e);
                }
            }
        }, FILES);
    }

    public static CompletableFuture<TransferStats> importFrom(TransferStorage storage, Path source, int parallelism) {
        if (parallelism < 1 || parallelism > 256) throw new IllegalArgumentException("Parallelism must be 1..256");
        return CompletableFuture.supplyAsync(() -> {
            Counters counts = new Counters();
            try {
                // Two buffered passes, constant memory. Legacy two-column mails get IDs
                // derived from file content + line number, stable on repeated imports.
                byte[] identity = digest(source);
                try (var reader = new BufferedReader(Files.newBufferedReader(source, StandardCharsets.UTF_8), BUFFER)) {
                    BoundedParallel.run(PARALLEL, new Records(reader, source, identity, counts), parallelism,
                            WINDOW_BYTES, line -> line.entry.payload().length, line -> line.entry.key(),
                            line -> attempt(storage, line.entry, 0),
                            (line, status) -> {
                                if (status.status() == TransferResult.Status.SUCCESS) {
                                    if (line.entry.kind() == TransferEntry.Kind.DATA) counts.data++;
                                    else counts.mails++;
                                } else {
                                    if (status.status() == TransferResult.Status.LOCKED) counts.locked++;
                                    else counts.failed++;
                                    log.error("Import skipped file={} line={} kind={} key={} status={} reason={}", source,
                                            line.number, line.entry.kind(), line.entry.key(), status.status(), status.detail());
                                }
                            }).get();
                }

                TransferStats stats = new TransferStats(counts.data, counts.mails, counts.locked, counts.failed);
                log.info("Repository import complete path={} result={}", source, stats);
                return stats;
            } catch (Exception e) {
                throw new CompletionException("Import aborted after partial progress: " + source
                        + " data=" + counts.data + " mails=" + counts.mails, e);
            }
        }, FILES);
    }

    private static CompletableFuture<TransferResult> attempt(TransferStorage storage, TransferEntry entry, int number) {
        CompletableFuture<TransferResult> request;
        try {
            request = storage.importEntry(entry).orTimeout(35, TimeUnit.SECONDS);
        } catch (Exception e) {
            request = CompletableFuture.failedFuture(e);
        }
        return request.handle((result, error) -> {
            TransferResult status = error == null && result != null ? result
                    : new TransferResult(TransferResult.Status.FAILED, error == null ? "No response" : error.toString());
            if (status.status() != TransferResult.Status.FAILED || number >= 2) return CompletableFuture.completedFuture(status);
            log.warn("Import retry kind={} key={} attempt={} reason={}", entry.kind(), entry.key(), number + 1, status.detail());
            var retry = new CompletableFuture<TransferResult>();
            RETRIES.schedule(() -> attempt(storage, entry, number + 1).whenComplete((value, failure) -> {
                if (failure == null) retry.complete(value);
                else retry.completeExceptionally(failure);
            }), (number + 1) * 500L, TimeUnit.MILLISECONDS);
            return retry;
        }).thenCompose(value -> value);
    }

    private static TransferEntry parse(String text, TransferEntry.Kind section, byte[] identity, long line) {
        if (section == null) throw new IllegalArgumentException("Expected #data or #mails section");
        int split = text.indexOf(' ');
        if (split < 0) throw new IllegalArgumentException("Expected UUID and Base64 separated by a space");
        UUID owner = UUID.fromString(text.substring(0, split));
        String body = text.substring(split + 1);
        UUID id = null;
        if (section == TransferEntry.Kind.MAIL) {
            int tail = body.indexOf(' ');
            if (tail >= 0) {
                id = UUID.fromString(body.substring(tail + 1).trim());
                body = body.substring(0, tail);
            } else {
                var seed = ByteBuffer.allocate(identity.length + Long.BYTES).put(identity).putLong(line).array();
                id = UUID.nameUUIDFromBytes(seed);
            }
        }
        int max = section == TransferEntry.Kind.DATA ? TransferEntry.MAX_DATA_BYTES : dev.by1337.sync.k2v.mail.Mail.MAX_PAYLOAD_BYTES;
        if (body.length() > (max + 2) / 3 * 4) throw new IllegalArgumentException("Oversized Base64 record");
        byte[] bytes = Base64.getDecoder().decode(body);
        if (section == TransferEntry.Kind.MAIL && !Arrays.equals(bytes, new String(bytes, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8)))
            throw new IllegalArgumentException("Mail payload is not valid UTF-8");
        return new TransferEntry(section, owner, bytes, id);
    }

    private static byte[] digest(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] buffer = new byte[BUFFER];
        try (var input = new BufferedInputStream(Files.newInputStream(file), BUFFER)) {
            int length;
            while ((length = input.read(buffer)) != -1) digest.update(buffer, 0, length);
        }
        return digest.digest();
    }

    private static String readLine(BufferedReader reader) throws IOException {
        StringBuilder result = new StringBuilder();
        boolean overflow = false;
        int value;
        while ((value = reader.read()) != -1 && value != '\n') {
            if (result.length() >= MAX_LINE) overflow = true;
            else if (!overflow) result.append((char) value);
        }
        if (overflow) throw new LineTooLong();
        if (value == -1 && result.length() == 0) return null;
        if (result.length() > 0 && result.charAt(result.length() - 1) == '\r') result.setLength(result.length() - 1);
        return result.toString();
    }

    /** Read lazily on the dedicated transfer worker, never on a game/network worker. */
    private static final class Records implements Iterator<Line> {
        private final BufferedReader reader;
        private final Path source;
        private final byte[] identity;
        private final Counters counts;
        private TransferEntry.Kind section;
        private long lineNumber;
        private Line next;
        private boolean ended;

        Records(BufferedReader reader, Path source, byte[] identity, Counters counts) {
            this.reader = reader;
            this.source = source;
            this.identity = identity;
            this.counts = counts;
        }

        @Override
        public boolean hasNext() {
            if (next != null) return true;
            if (ended) return false;
            while (true) {
                String text;
                try {
                    text = readLine(reader);
                } catch (LineTooLong e) {
                    lineNumber++;
                    counts.failed++;
                    log.error("Import rejected oversized line={} file={}", lineNumber, source);
                    continue;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (text == null) {
                    ended = true;
                    return false;
                }
                lineNumber++;
                if (text.isBlank()) continue;
                if (text.equals("#data")) {
                    section = TransferEntry.Kind.DATA;
                    continue;
                }
                if (text.equals("#mails")) {
                    section = TransferEntry.Kind.MAIL;
                    continue;
                }
                if (text.startsWith("#bsync-transfer ") && !text.equals("#bsync-transfer 1"))
                    throw new IllegalArgumentException("Unsupported dump version: " + text);
                if (text.startsWith("#")) continue;
                try {
                    next = new Line(lineNumber, parse(text, section, identity, lineNumber));
                    return true;
                } catch (RuntimeException e) {
                    counts.failed++;
                    log.error("Import rejected line={} file={} reason={}", lineNumber, source, e.getMessage());
                }
            }
        }

        @Override
        public Line next() {
            if (!hasNext()) throw new NoSuchElementException();
            Line value = next;
            next = null;
            return value;
        }
    }

    private record Line(long number, TransferEntry entry) {
    }

    private static class Counters {
        long data, mails, locked, failed;
    }

    private static class LineTooLong extends IOException {
    }
}
