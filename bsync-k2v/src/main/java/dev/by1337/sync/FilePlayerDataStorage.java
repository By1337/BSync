package dev.by1337.sync;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;

public class FilePlayerDataStorage {
    private static final Logger log = LoggerFactory.getLogger(FilePlayerDataStorage.class);
    private final Object[] locks;
    private final Path dataFolder;

    public FilePlayerDataStorage(File dataFolder) {
        dataFolder.mkdirs();
        this.dataFolder = dataFolder.toPath();
        locks = new Object[512];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
    }

    public void write(UUID key, byte[] data) {
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
        try {
            synchronized (route(key)) {
                var mails = dataFolder.resolve(key + ".mailbox");
                try (FileWriter w = new FileWriter(mails.toFile(), StandardCharsets.UTF_8, true)) {
                    w.write(mail.replace("\n", "\\n"));
                    w.append('\n');
                }
            }
        } catch (IOException e) {
            log.error("Failed to write mail {} {}", mail, dataFolder.resolve(key + ".mailbox"), e);
        }
    }

    public List<String> readAllMailsAndDelete(UUID key) {
        try {
            synchronized (route(key)) {
                var path = dataFolder.resolve(key + ".mailbox");
                if (Files.exists(path)) {
                    var v = Files.readAllLines(path).stream().map(s -> s.replace("\\n", "\n")).toList();
                    Files.delete(path);
                    return v;
                }
            }
        } catch (Exception e) {
            log.error("Failed to read data {}", dataFolder.resolve(key + ".dat"), e);
        }
        return List.of();
    }

    private Object route(UUID uuid) {
        return locks[Math.floorMod(hash(uuid), locks.length)];
    }

    static long hash(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }
}
