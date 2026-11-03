package dev.by1337.sync.k2v.storage.sql;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.transfer.*;
import dev.by1337.sync.k2v.transfer.RepositoryTransfer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class SqliteLeaseIntegrationTest {
    @TempDir
    Path directory;

    private SqlStorageConfig config(String file, String options) {
        return SqlStorageConfig.parse("sqlite://" + directory.resolve(file).toUri().getRawPath() + options);
    }

    private Handle open(SqlStorageConfig config) throws Exception {
        // Keep the SQLite busy timeout, but allow cold JDBC/native initialization
        // and pool scheduling to take longer than the 250 ms contention window.
        // Hikari caches this timeout at startup, so configure it before creating the pool.
        var pool = SqlDataSources.create(config, config.poolSize, "sqlite-test", 5_000);
        var database = new SqlLeaseRepository(pool, pool, config);
        try {
            database.initialize(true);
            return new Handle(pool, database);
        } catch (Exception e) {
            database.close();
            pool.close();
            throw e;
        }
    }

    @Test
    void dataPendingMailAndDedupReceiptsSurviveRestart() throws Exception {
        var config = config("nested/players + база.db", "");
        UUID player = UUID.randomUUID(), token = UUID.randomUUID();
        Mail delivered = new Mail(UUID.randomUUID(), player, "delivered"), pending = new Mail(UUID.randomUUID(), player, "pending");
        long epoch;
        try (var handle = open(config)) {
            var db = handle.database;
            epoch = db.claim(player, token).epoch();
            assertTrue(db.admit(delivered));
            assertTrue(db.acknowledge(db.pending(List.of(token)).get(0)));
            assertTrue(db.admit(pending));
            assertTrue(db.save(player, token, epoch, new byte[]{ 7 }, false));
            // Simulate an abandoned session: close without releasing the player token.
        }
        try (var handle = open(config)) {
            var db = handle.database;
            UUID replacement = UUID.randomUUID();
            var recovered = db.claim(player, replacement);
            assertNotNull(recovered);
            assertTrue(recovered.epoch() > epoch);
            assertArrayEquals(new byte[]{ 7 }, recovered.data());
            assertFalse(db.save(player, token, epoch, new byte[]{ 9 }, true));
            assertTrue(db.admit(delivered));
            assertFalse(db.admit(new Mail(delivered.id(), player, "conflict")));
            assertEquals(List.of(pending), db.pending(List.of(replacement)).stream().map(SqlLeaseRepository.PendingMail::mail).toList());
            try (var connection = handle.pool.getConnection(); var query = connection.createStatement()) {
                try (var rows = query.executeQuery("PRAGMA journal_mode")) {
                    assertTrue(rows.next());
                    assertEquals("wal", rows.getString(1));
                }
                try (var rows = query.executeQuery("PRAGMA synchronous")) {
                    assertTrue(rows.next());
                    assertEquals(2, rows.getInt(1));
                }
            }
        }
    }

    @Test
    void duplicateRepositoryOpenIsRejectedUntilOwnerCloses() throws Exception {
        var config = config("exclusive.db", "");
        try (var first = open(config)) {
            UUID player = UUID.randomUUID(), token = UUID.randomUUID();
            var lease = first.database.claim(player, token);
            assertThrows(SQLException.class, () -> {
                try (var ignored = open(config)) {
                }
            });
            assertTrue(first.database.save(player, token, lease.epoch(), new byte[]{ 3 }, false));
        }
        try (var reopened = open(config)) {
            assertNotNull(reopened.database.claim(UUID.randomUUID(), UUID.randomUUID()));
        }
    }

    @Test
    void namespacesInOneFileHaveIndependentOwnershipAndMailIds() throws Exception {
        try (var a = open(config("shared.db", "?repository=left")); var b = open(config("shared.db", "?repository=right"))) {
            UUID player = UUID.randomUUID(), id = UUID.randomUUID(), token = UUID.randomUUID();
            assertNotNull(a.database.claim(player, token));
            assertNotNull(b.database.claim(player, token));
            assertTrue(a.database.admit(new Mail(id, player, "left")));
            assertTrue(b.database.admit(new Mail(id, player, "right")));
            assertEquals("left", a.database.pending(List.of(token)).get(0).mail().payload());
            assertEquals("right", b.database.pending(List.of(token)).get(0).mail().payload());
        }
    }

    @Test
    void failedMailRetainsOrderAndStaleSessionCannotAcknowledgeIt() throws Exception {
        try (var handle = open(config("mail.db", ""))) {
            var db = handle.database;
            UUID player = UUID.randomUUID(), old = UUID.randomUUID(), fresh = UUID.randomUUID();
            var lease = db.claim(player, old);
            Mail first = new Mail(UUID.randomUUID(), player, "first"), second = new Mail(UUID.randomUUID(), player, "second");
            db.admit(first);
            db.admit(second);
            var pending = db.pending(List.of(old)).get(0);
            db.failed(pending, "business failure", 60_000);
            assertTrue(db.pending(List.of(old)).isEmpty());
            assertTrue(db.save(player, old, lease.epoch(), null, true));
            assertNotNull(db.claim(player, fresh));
            assertFalse(db.acknowledge(pending));
            try (var connection = handle.pool.getConnection(); var update = connection.createStatement()) {
                update.executeUpdate("UPDATE " + SqlLeaseRepository.MAILS + " SET next_attempt=0");
            }
            var retried = db.pending(List.of(fresh)).get(0);
            assertEquals(first, retried.mail());
            assertEquals(1, retried.attempts());
            assertTrue(db.acknowledge(retried));
            assertEquals(second, db.pending(List.of(fresh)).get(0).mail());
        }
    }

    @Test
    void concurrentAcquisitionAndFinalSaveRespectSessionTokens() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try (var handle = open(config("claims.db", "?pool=2"))) {
            UUID player = UUID.randomUUID(), first = UUID.randomUUID(), second = UUID.randomUUID();
            var start = new CountDownLatch(1);
            var a = executor.submit(() -> {
                start.await();
                return handle.database.claim(player, first);
            });
            var b = executor.submit(() -> {
                start.await();
                return handle.database.claim(player, second);
            });
            start.countDown();
            var left = a.get(5, TimeUnit.SECONDS);
            var right = b.get(5, TimeUnit.SECONDS);
            assertNotEquals(left == null, right == null);
            UUID winner = left != null ? first : second;
            long epoch = (left != null ? left : right).epoch();
            assertTrue(handle.database.save(player, winner, epoch, new byte[]{ 7 }, true));
            var next = handle.database.claim(player, UUID.randomUUID());
            assertArrayEquals(new byte[]{ 7 }, next.data());
            assertFalse(handle.database.save(player, winner, epoch, new byte[]{ 9 }, false));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void poolWaitTimeoutIsIndependentOfSqliteBusyTimeout() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var handle = open(config("pool-wait.db", "?timeout=250ms&pool=1"))) {
            Future<Boolean> waiting;
            try (var held = handle.pool.getConnection()) {
                try (var query = held.createStatement(); var rows = query.executeQuery("PRAGMA busy_timeout")) {
                    assertTrue(rows.next());
                    assertEquals(250, rows.getInt(1));
                }
                var started = new CountDownLatch(1);
                waiting = executor.submit(() -> {
                    started.countDown();
                    try (var connection = handle.pool.getConnection()) {
                        return connection.isValid(1);
                    }
                });
                assertTrue(started.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> waiting.get(500, TimeUnit.MILLISECONDS));
            }
            assertTrue(waiting.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void competingWriterTimesOutAndCanRetryAfterRollback() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var handle = open(config("busy.db", "?timeout=250ms&pool=2"))) {
            var db = handle.database;
            UUID player = UUID.randomUUID(), token = UUID.randomUUID();
            var lease = db.claim(player, token);
            try (var writer = handle.pool.getConnection()) {
                try (var warm = handle.pool.getConnection()) {
                    assertTrue(warm.isValid(1));
                }
                writer.setAutoCommit(false);
                var failure = executor.submit(() -> {
                    try {
                        db.save(player, token, lease.epoch(), new byte[]{ 2 }, false);
                        return null;
                    } catch (SQLException e) {
                        return e;
                    }
                }).get(2, TimeUnit.SECONDS);
                assertNotNull(failure);
                assertEquals(5, failure.getErrorCode(), "SQLITE_BUSY");
                writer.rollback();
                writer.setAutoCommit(true);
            }
            assertTrue(db.save(player, token, lease.epoch(), new byte[]{ 2 }, false));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void storageRetriesHandlerAndExportsImportsActualFile() throws Exception {
        var config = config("storage.db", "?poll=1s&acquireWait=0s");
        UUID player = UUID.randomUUID(), mailId = UUID.randomUUID();
        Path dump = directory.resolve("dump.txt");
        try (var handle = storage(config)) {
            var storage = handle.storage;
            var entry = new TransferEntry(TransferEntry.Kind.DATA, player, new byte[]{ 3 }, null);
            assertEquals(TransferResult.Status.SUCCESS, storage.importEntry(entry).get().status());
            assertEquals(TransferResult.Status.SUCCESS, storage.importEntry(new TransferEntry(TransferEntry.Kind.MAIL,
                    player, "reward".getBytes(java.nio.charset.StandardCharsets.UTF_8), mailId)).get().status());
            RepositoryTransfer.exportTo(storage, dump).get(5, TimeUnit.SECONDS);
            var loaded = new CompletableFuture<Boolean>();
            storage.lockAndLoadData(player, (okay, data) -> loaded.complete(okay));
            assertTrue(loaded.get(5, TimeUnit.SECONDS));
            assertEquals(TransferResult.Status.LOCKED, storage.importEntry(entry).get().status());
            var ids = new CopyOnWriteArrayList<UUID>();
            var delivered = new CountDownLatch(1);
            storage.setMailHandler(mail -> {
                ids.add(mail.id());
                if (ids.size() == 1) throw new IllegalStateException("temporary");
                delivered.countDown();
            });
            storage.doMailsLoad(player);
            assertTrue(delivered.await(8, TimeUnit.SECONDS));
            assertEquals(List.of(mailId, mailId), ids);
        }
        try (var handle = storage(config("imported.db", ""))) {
            RepositoryTransfer.importFrom(handle.storage, dump, 4).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{ 3 }, handle.storage.loadSnapshot(player).get());
            var mails = handle.storage.exportSources().get(0).next(TransferEntry.Kind.MAIL).get().entries();
            assertEquals(1, mails.size());
            assertEquals(mailId, mails.get(0).messageId());
        }
    }

    private StorageHandle storage(SqlStorageConfig config) throws Exception {
        var storage = new JdbcPlayerDataStorage(config, directory.resolve(UUID.randomUUID().toString()), true, true);
        try {
            storage.initialized().get(5, TimeUnit.SECONDS);
            return new StorageHandle(storage);
        } catch (Exception e) {
            storage.close();
            throw e;
        }
    }

    private record StorageHandle(JdbcPlayerDataStorage storage) implements AutoCloseable {
        public void close() {
            storage.close();
        }
    }

    private record Handle(HikariDataSource pool, SqlLeaseRepository database) implements AutoCloseable {
        public void close() throws Exception {
            pool.close();
            database.close();
        }
    }
}
