package dev.by1337.sync.common.mail;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

import static java.nio.file.StandardOpenOption.*;

/** Synchronous durable admission; records remain until explicitly acknowledged. */
public final class MailJournal implements AutoCloseable {
    private final Path directory;
    private final Map<UUID, Entry> pending = new LinkedHashMap<>();
    private final FileChannel lockChannel;
    private final FileLock lock;
    private long sequence;
    private boolean closed;

    /** A null directory is intended for isolated, nonpersistent transports/tests only. */
    public MailJournal(Path directory) {
        this.directory = directory;
        try {
            if (directory == null) {
                lockChannel = null;
                lock = null;
                return;
            }
            Files.createDirectories(directory);
            lockChannel = FileChannel.open(directory.resolve("journal.lock"), CREATE, WRITE);
            FileLock acquired;
            try {
                acquired = lockChannel.tryLock();
                if (acquired == null) throw new IOException("Mailbox journal is already open: " + directory);
            } catch (IOException | RuntimeException e) {
                lockChannel.close();
                throw e;
            }
            lock = acquired;
            try (var paths = Files.list(directory)) {
                for (Path path : paths.sorted().toList()) {
                    String filename = path.getFileName().toString();
                    if (filename.matches("[0-9]{19}-.*\\.mail(?:\\.tmp)?"))
                        sequence = Math.max(sequence, Long.parseLong(filename.substring(0, 19)));
                    if (!filename.endsWith(".mail")) continue;
                    try (var in = new DataInputStream(Files.newInputStream(path))) {
                        if (in.readInt() != 1) throw new IOException("Invalid mailbox journal version: " + path);
                        Mail mail = new Mail(new UUID(in.readLong(), in.readLong()),
                                new UUID(in.readLong(), in.readLong()), readPayload(in));
                        pending.put(mail.id(), new Entry(mail, path));
                        sequence = Math.max(sequence, Long.parseLong(path.getFileName().toString().substring(0, 19)));
                    }
                }
            } catch (IOException | RuntimeException e) {
                lock.release();
                lockChannel.close();
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open mailbox journal " + directory, e);
        }
    }

    private static String readPayload(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 1_048_576) throw new IOException("Invalid mail size " + length);
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) throw new EOFException("Truncated mailbox journal");
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public synchronized Mail append(UUID owner, String payload) {
        Mail mail = new Mail(UUID.randomUUID(), owner, payload);
        append(mail);
        return mail;
    }

    public synchronized void append(Mail mail) {
        if (closed) throw new IllegalStateException("Mailbox journal is closed");
        if (pending.containsKey(mail.id())) {
            if (!pending.get(mail.id()).mail.equals(mail)) throw new IllegalArgumentException("Conflicting mail ID " + mail.id());
            return;
        }
        Path path = null;
        if (directory != null) {
            path = directory.resolve(String.format(Locale.ROOT, "%019d-%s.mail", ++sequence, mail.id()));
            Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
            try {
                var bytes = new ByteArrayOutputStream();
                try (var out = new DataOutputStream(bytes)) {
                    out.writeInt(1);
                    out.writeLong(mail.id().getMostSignificantBits());
                    out.writeLong(mail.id().getLeastSignificantBits());
                    out.writeLong(mail.owner().getMostSignificantBits());
                    out.writeLong(mail.owner().getLeastSignificantBits());
                    byte[] payload = mail.payload().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(payload.length);
                    out.write(payload);
                }
                try (var file = FileChannel.open(temporary, CREATE_NEW, WRITE)) {
                    ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
                    while (buffer.hasRemaining()) file.write(buffer);
                    file.force(true);
                }
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
                forceDirectory();
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot persist mail " + mail.id(), e);
            }
        }
        pending.put(mail.id(), new Entry(mail, path));
    }

    public synchronized List<Mail> pending() {
        return pending.values().stream().map(Entry::mail).toList();
    }

    public synchronized void acknowledge(UUID id) {
        Entry entry = pending.get(id);
        if (entry == null) return;
        try {
            if (entry.path != null) {
                Files.deleteIfExists(entry.path);
                forceDirectory();
            }
            pending.remove(id);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot acknowledge mail " + id, e);
        }
    }

    private void forceDirectory() throws IOException {
        try (var channel = FileChannel.open(directory, READ)) {
            channel.force(true);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (lock != null) lock.release();
            if (lockChannel != null) lockChannel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record Entry(Mail mail, Path path) {
    }
}
