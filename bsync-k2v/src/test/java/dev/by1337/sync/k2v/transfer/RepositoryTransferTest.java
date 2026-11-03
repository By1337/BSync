package dev.by1337.sync.k2v.transfer;

import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.common.transfer.*;
import dev.by1337.sync.k2v.storage.FilePlayerDataStorage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class RepositoryTransferTest {
    @TempDir
    Path directory;

    @Test
    void localRoundTripPreservesBinaryEmptySnapshotsUtf8AndStableMailIds() throws Exception {
        var source = new FilePlayerDataStorage(directory.resolve("source").toFile());
        var target = new FilePlayerDataStorage(directory.resolve("target").toFile());
        UUID player = UUID.randomUUID();
        UUID empty = UUID.randomUUID();
        UUID mailId = UUID.randomUUID();
        try {
            source.pushSnapshot(player, new byte[]{ 0, -1, 0, 42 });
            source.pushSnapshot(empty, new byte[0]);
            assertEquals(TransferResult.Status.SUCCESS, source.importEntry(new TransferEntry(
                    TransferEntry.Kind.MAIL, player, "line one\nстрока два".getBytes(StandardCharsets.UTF_8), mailId)).get().status());
            source.importEntry(new TransferEntry(TransferEntry.Kind.MAIL, player, new byte[0], UUID.randomUUID())).get();
            Path dump = directory.resolve("export.txt");
            assertEquals(new TransferStats(2, 2, 0, 0), RepositoryTransfer.exportTo(source, dump).get(10, TimeUnit.SECONDS));
            String text = Files.readString(dump);
            assertTrue(text.contains("#data\n"));
            assertTrue(text.contains("#mails\n"));
            assertTrue(text.contains(mailId.toString()));
            assertEquals(new TransferStats(2, 2, 0, 0), RepositoryTransfer.importFrom(target, dump, 4).get(10, TimeUnit.SECONDS));
            assertArrayEquals(new byte[]{ 0, -1, 0, 42 }, target.loadSnapshot(player).join());
            assertArrayEquals(new byte[0], target.loadSnapshot(empty).join());
            // Replaying the same file must not duplicate pending mails.
            RepositoryTransfer.importFrom(target, dump, 4).get(10, TimeUnit.SECONDS);
            Path second = directory.resolve("second.txt");
            assertEquals(2, RepositoryTransfer.exportTo(target, second).get().mails());
            assertTrue(Files.readString(second).contains(mailId.toString()));
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    void importLogsMalformedAndLockedRowsWhileContinuingValidRecords() throws Exception {
        UUID locked = UUID.randomUUID(), okay = UUID.randomUUID();
        Path input = directory.resolve("input.txt");
        Files.writeString(input, "#data\n" + locked + " AQ==\nmalformed\n" + okay + " Ag==\n");
        var target = new FilePlayerDataStorage(directory.resolve("target").toFile());
        try {
            target.pushSnapshot(locked, new byte[]{ 7 });
            target.setLockValidator(locked::equals);
            var stats = RepositoryTransfer.importFrom(target, input, 8).get(10, TimeUnit.SECONDS);
            assertEquals(new TransferStats(1, 0, 1, 1), stats);
            assertArrayEquals(new byte[]{ 7 }, target.loadSnapshot(locked).join());
            assertArrayEquals(new byte[]{ 2 }, target.loadSnapshot(okay).join());
        } finally {
            target.close();
        }
    }

    @Test
    void legacyTwoColumnMailsHaveStableDistinctIdsOnRepeatedImport() throws Exception {
        UUID owner = UUID.randomUUID();
        Path file = directory.resolve("legacy.txt");
        Files.writeString(file, "#mails\n" + owner + " aGk=\n" + owner + " aGk=\n");
        var ids = new ArrayList<UUID>();
        TransferStorage target = new TransferStorage() {
            public CompletableFuture<TransferResult> importEntry(TransferEntry entry) {
                ids.add(entry.messageId());
                return CompletableFuture.completedFuture(TransferResult.success());
            }
        };
        assertEquals(2, RepositoryTransfer.importFrom(target, file, 256).get().mails());
        assertEquals(2, RepositoryTransfer.importFrom(target, file, 256).get().mails());
        assertNotEquals(ids.get(0), ids.get(1));
        assertEquals(ids.subList(0, 2), ids.subList(2, 4));
    }

    @Test
    void exportFailurePreservesExistingDestinationAndClosesSource() throws Exception {
        var closed = new java.util.concurrent.atomic.AtomicBoolean();
        Path file = directory.resolve("existing.txt");
        Files.writeString(file, "old complete export");
        TransferStorage broken = new TransferStorage() {
            public List<ExportSource> exportSources() {
                return List.of(new ExportSource() {
                    public CompletableFuture<S2CExportPagePacket> next(TransferEntry.Kind kind) {
                        return CompletableFuture.completedFuture(S2CExportPagePacket.failed("database down"));
                    }

                    public void close() {
                        closed.set(true);
                    }
                });
            }
        };
        assertThrows(ExecutionException.class, () -> RepositoryTransfer.exportTo(broken, file).get());
        assertEquals("old complete export", Files.readString(file));
        assertTrue(closed.get());
    }

    @Test
    void importRetriesFailuresButDoesNotRetryLocksAndReturnsFinalStats() throws Exception {
        UUID owner = UUID.randomUUID();
        Path file = directory.resolve("retry.txt");
        Files.writeString(file, "#data\n" + owner + " AQ==\n");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        TransferStorage flaky = new TransferStorage() {
            public CompletableFuture<TransferResult> importEntry(TransferEntry entry) {
                return CompletableFuture.completedFuture(calls.incrementAndGet() < 3
                        ? new TransferResult(TransferResult.Status.FAILED, "temporary failure") : TransferResult.success());
            }
        };
        assertEquals(new TransferStats(1, 0, 0, 0), RepositoryTransfer.importFrom(flaky, file, 2).get(10, TimeUnit.SECONDS));
        assertEquals(3, calls.get());
    }

    @Test
    void deliveredLocalMailIsNotResurrectedByImportAfterRestart() throws Exception {
        UUID owner = UUID.randomUUID(), id = UUID.randomUUID();
        Path folder = directory.resolve("target");
        var target = new FilePlayerDataStorage(folder.toFile());
        try {
            var delivered = new CountDownLatch(1);
            target.setLockValidator(owner::equals);
            target.setMailHandler(mail -> delivered.countDown());
            target.importEntry(new TransferEntry(TransferEntry.Kind.MAIL, owner, "reward".getBytes(StandardCharsets.UTF_8), id)).get();
            target.doMailsLoad(owner);
            assertTrue(delivered.await(5, TimeUnit.SECONDS));
            // Wait until the storage-owned delivery task has durably acknowledged the mail.
            var field = FilePlayerDataStorage.class.getDeclaredField("mailWorker");
            field.setAccessible(true);
            ((ExecutorService) field.get(target)).submit(() -> {
            }).get();
        } finally {
            target.close();
        }
        target = new FilePlayerDataStorage(folder.toFile());
        try {
            target.importEntry(new TransferEntry(TransferEntry.Kind.MAIL, owner, "reward".getBytes(StandardCharsets.UTF_8), id)).get();
            assertEquals(0, RepositoryTransfer.exportTo(target, directory.resolve("empty.txt")).get().mails());
        } finally {
            target.close();
        }
    }

    @Test
    void exportContinuesAfterEmptyFilteredPageWithAdvancedCursor() throws Exception {
        UUID skipped = new UUID(0, 1), matching = new UUID(0, 2);
        TransferStorage storage = new TransferStorage() {
            public List<ExportSource> exportSources() {
                return List.of(new ExportSource() {
                    int page;

                    public CompletableFuture<S2CExportPagePacket> next(TransferEntry.Kind kind) {
                        if (kind == TransferEntry.Kind.MAIL) return CompletableFuture.completedFuture(S2CExportPagePacket.empty());
                        return CompletableFuture.completedFuture(page++ == 0
                                ? new S2CExportPagePacket(List.of(), skipped, 0, false, "")
                                : new S2CExportPagePacket(List.of(new TransferEntry(kind, matching, new byte[]{ 1 }, null)), matching, 0, true, ""));
                    }
                });
            }
        };
        Path output = directory.resolve("filtered.txt");
        assertEquals(new TransferStats(1, 0, 0, 0), RepositoryTransfer.exportTo(storage, output).get(5, TimeUnit.SECONDS));
        assertTrue(Files.readString(output).contains(matching + " AQ=="));
        assertFalse(Files.readString(output).contains(skipped.toString()));
    }

}
