package dev.by1337.sync.k2v.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class FilePlayerDataStorageTest {
    @TempDir
    Path directory;
    private FilePlayerDataStorage storage;
    private final UUID player = new UUID(Long.MIN_VALUE, 37);
    private final UUID other = new UUID(13, 42);

    @BeforeEach
    void setUp() {
        storage = new FilePlayerDataStorage(directory.resolve("players").toFile());
    }

    @AfterEach
    void closeStorage() {
        storage.close();
    }

    @Test
    void missingPlayerHasNoSnapshotOrMail() {
        assertNull(storage.loadSnapshot(player).join());
        assertEquals(List.of(), storage.readAllMailsAndDelete(player));
        int version = storage.lockAndLoadData(player, (locked, data) -> {
            assertTrue(locked);
            assertNull(data);
        });
        assertEquals(1, version);
    }

    @Test
    void overwritesSnapshotAndPersistsAcrossStorageInstances() {
        storage.pushSnapshot(player, new byte[]{ 13, 37 });
        storage.pushSnapshot(other, new byte[]{ 42 });
        storage.pushSnapshot(player, new byte[]{ 0, -1, 7 });
        storage.close();
        storage = new FilePlayerDataStorage(directory.resolve("players").toFile());
        FilePlayerDataStorage reopened = storage;
        assertArrayEquals(new byte[]{ 0, -1, 7 }, reopened.loadSnapshot(player).join());
        assertArrayEquals(new byte[]{ 42 }, reopened.loadSnapshot(other).join());
        assertFalse(Files.exists(directory.resolve("players/" + player + ".dat.tmp")));
    }

    @Test
    void nullSnapshotLeavesStoredValueAndEmptySnapshotIsPreserved() {
        storage.pushSnapshot(player, new byte[]{ 13 });
        storage.pushSnapshot(player, null);
        assertArrayEquals(new byte[]{ 13 }, storage.loadSnapshot(player).join());
        storage.pushSnapshot(player, new byte[0]);
        assertArrayEquals(new byte[0], storage.loadSnapshot(player).join());
    }

    @Test
    void drainsMailInOrderAndKeepsOtherPlayersMail() {
        List<String> mails = List.of("Hi", "line one\nline two", "");
        mails.forEach(mail -> storage.pushMail(player, mail));
        storage.pushMail(other, "other");
        assertEquals(mails, storage.readAllMailsAndDelete(player));
        assertEquals(List.of(), storage.readAllMailsAndDelete(player));
        assertFalse(Files.exists(directory.resolve("players/" + player + ".mailbox")));
        assertEquals(List.of("other"), storage.readAllMailsAndDelete(other));
    }

    @Test
    void mailLoaderDeliversOwnerAndDoesNotRedeliverConsumedMail() throws Exception {
        List<String> delivered = new java.util.concurrent.CopyOnWriteArrayList<>();
        storage.setLockValidator(key -> true);
        storage.setMailAccept((owner, mail) -> {
            assertEquals(player, owner);
            delivered.add(mail);
        });
        storage.pushMail(player, "first");
        storage.pushMail(player, "second");
        storage.doMailsLoad(player);
        storage.doMailsLoad(player);
        drainWorker();
        assertEquals(List.of("first", "second"), delivered);
    }

    @Test
    void failedHandlerRetainsMessageAcrossRestartWithSameId() throws Exception {
        var failed = new java.util.concurrent.atomic.AtomicReference<java.util.UUID>();
        storage.setLockValidator(key -> true);
        storage.setMailHandler(mail -> {
            failed.set(mail.id());
            throw new IllegalStateException("temporary handler failure");
        });
        storage.pushMail(player, "reward");
        storage.doMailsLoad(player);
        drainWorker();
        assertNotNull(failed.get());
        storage.close();
        storage = new FilePlayerDataStorage(directory.resolve("players").toFile());
        storage.setLockValidator(key -> true);
        var received = new java.util.concurrent.atomic.AtomicReference<dev.by1337.sync.k2v.mail.Mail>();
        storage.setMailHandler(received::set);
        storage.doMailsLoad(player);
        drainWorker();
        assertEquals(failed.get(), received.get().id());
        assertEquals("reward", received.get().payload());
        assertTrue(storage.readAllMailsAndDelete(player).isEmpty());
    }

    @Test
    void migratesLegacyMailboxFileBeforeDeletingIt() throws Exception {
        storage.close();
        var legacy = directory.resolve("players/" + player + ".mailbox");
        Files.writeString(legacy, "first\nline one\\nline two\n");
        storage = new FilePlayerDataStorage(directory.resolve("players").toFile());
        assertFalse(Files.exists(legacy));
        assertEquals(List.of("first", "line one\nline two"), storage.readAllMailsAndDelete(player));
    }

    private void drainWorker() throws Exception {
        var field = FilePlayerDataStorage.class.getDeclaredField("mailWorker");
        field.setAccessible(true);
        ((java.util.concurrent.ExecutorService) field.get(storage)).submit(() -> {
        }).get(5, java.util.concurrent.TimeUnit.SECONDS);
    }

}
