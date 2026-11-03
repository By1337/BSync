package dev.by1337.sync.k2v.storage.sql;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.transfer.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Opt in against a disposable database with BSYNC_SQL_TEST_PORT and BSYNC_SQL_TEST_SCHEME. */
class SqlLeaseIntegrationTest {
    @TempDir
    Path directory;
    private SqlStorageConfig config;
    private HikariDataSource data, heartbeat;
    private SqlLeaseRepository database;
    private final UUID player = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {
        String port = System.getenv("BSYNC_SQL_TEST_PORT");
        Assumptions.assumeTrue(port != null, "Requires a disposable MySQL/MariaDB database");
        String scheme = System.getenv().getOrDefault("BSYNC_SQL_TEST_SCHEME", "mariadb");
        config = SqlStorageConfig.parse(scheme + "://127.0.0.1:" + port + "/bsync_test/test_"
                + UUID.randomUUID().toString().replace("-", "")
                + "?user=root&password=bsync-test-only&timeout=2s&lease=30s&renew=25s&poll=1s&acquireWait=0s");
        data = SqlDataSources.create(config, 4, "sql-test-data");
        heartbeat = SqlDataSources.create(config, 1, "sql-test-renew");
        database = new SqlLeaseRepository(data, heartbeat, config);
        database.initialize(true);
    }

    @AfterEach
    void close() throws Exception {
        if (data != null) {
            try (var connection = data.getConnection()) {
                for (String table : List.of(SqlLeaseRepository.MAILS, SqlLeaseRepository.PLAYERS)) {
                    try (var sql = connection.prepareStatement("DELETE FROM " + table + " WHERE repository=?")) {
                        sql.setString(1, config.repository);
                        sql.executeUpdate();
                    }
                }
            } finally {
                data.close();
                heartbeat.close();
            }
        }
    }

    @Test
    void concurrentAcquirersHaveExactlyOneWinner() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        UUID first = UUID.randomUUID(), second = UUID.randomUUID();
        try {
            var a = executor.submit(() -> {
                start.await();
                return database.claim(player, first);
            });
            var b = executor.submit(() -> {
                start.await();
                return database.claim(player, second);
            });
            start.countDown();
            var left = a.get(5, TimeUnit.SECONDS);
            var right = b.get(5, TimeUnit.SECONDS);
            assertNotEquals(left == null, right == null);
            assertEquals(1, (left != null ? left : right).epoch());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void expiredOwnerCannotRenewSaveOrReleaseAnotherOwnersLease() throws Exception {
        UUID old = UUID.randomUUID();
        var original = database.claim(player, old);
        assertNotNull(original);
        assertTrue(database.save(player, old, original.epoch(), new byte[]{ 1 }, false));
        assertTrue(database.save(player, old, original.epoch(), new byte[]{ 1 }, false), "Unchanged data still matches ownership");
        expire(player);
        assertFalse(database.renew(player, old, original.epoch()));
        assertFalse(database.save(player, old, original.epoch(), new byte[]{ 9 }, false));
        UUID fresh = UUID.randomUUID();
        var replacement = database.claim(player, fresh);
        assertNotNull(replacement);
        assertTrue(replacement.epoch() > original.epoch());
        assertArrayEquals(new byte[]{ 1 }, replacement.data());
        assertTrue(database.save(player, fresh, replacement.epoch(), new byte[]{ 2 }, false));
        assertFalse(database.save(player, old, original.epoch(), new byte[]{ 9 }, true));
        assertTrue(database.renew(player, fresh, replacement.epoch()));
        assertArrayEquals(new byte[]{ 2 }, database.snapshot(player));
    }

    @Test
    void resumingAnUnconfirmedAcquisitionDoesNotIncrementEpochOrStealIt() throws Exception {
        UUID token = UUID.randomUUID();
        var first = database.claim(player, token);
        var replay = database.claim(player, token);
        assertNotNull(replay);
        assertEquals(first.epoch(), replay.epoch());
        assertTrue(replay.remainingMillis() <= config.leaseMillis);
        assertNull(database.claim(player, UUID.randomUUID()));
    }

    @Test
    void finalSnapshotAndLeaseReleaseAreOneMutation() throws Exception {
        UUID token = UUID.randomUUID();
        var lease = database.claim(player, token);
        assertTrue(database.save(player, token, lease.epoch(), new byte[]{ 7 }, true));
        var next = database.claim(player, UUID.randomUUID());
        assertNotNull(next);
        assertArrayEquals(new byte[]{ 7 }, next.data());
        assertFalse(database.save(player, token, lease.epoch(), new byte[]{ 8 }, false));
    }

    @Test
    void mailboxAdmissionDeduplicationAndAcknowledgementAreFenced() throws Exception {
        UUID old = UUID.randomUUID();
        var lease = database.claim(player, old);
        var mail = new Mail(UUID.randomUUID(), player, "строка\nreward");
        assertTrue(database.admit(mail));
        assertTrue(database.admit(mail));
        assertFalse(database.admit(new Mail(mail.id(), player, "different")));
        var pending = database.pending(List.of(old));
        assertEquals(1, pending.size());
        assertEquals(mail, pending.get(0).mail());
        expire(player);
        assertFalse(database.acknowledge(pending.get(0)));
        UUID fresh = UUID.randomUUID();
        assertNotNull(database.claim(player, fresh));
        var next = database.pending(List.of(fresh));
        assertEquals(mail.id(), next.get(0).mail().id());
        assertTrue(database.acknowledge(next.get(0)));
        assertTrue(database.admit(mail));
        assertTrue(database.pending(List.of(fresh)).isEmpty());
        assertTrue(database.exportMails(0).entries().isEmpty());
    }

    @Test
    void failedMailBlocksOnlyItsOwnPlayerAndRetainsStableId() throws Exception {
        UUID other = UUID.randomUUID(), firstToken = UUID.randomUUID(), secondToken = UUID.randomUUID();
        database.claim(player, firstToken);
        database.claim(other, secondToken);
        Mail first = new Mail(UUID.randomUUID(), player, "first"), second = new Mail(UUID.randomUUID(), player, "second"),
                independent = new Mail(UUID.randomUUID(), other, "independent");
        database.admit(first);
        database.admit(second);
        database.admit(independent);
        var pending = database.pending(List.of(firstToken, secondToken));
        assertEquals(2, pending.size());
        var failed = pending.stream().filter(row -> row.mail().id().equals(first.id())).findFirst().orElseThrow();
        database.failed(failed, "handler failed", 60_000);
        var eligible = database.pending(List.of(firstToken, secondToken));
        assertEquals(1, eligible.size());
        assertEquals(independent.id(), eligible.get(0).mail().id());
        try (var connection = data.getConnection(); var sql = connection.prepareStatement("UPDATE " + SqlLeaseRepository.MAILS
                + " SET next_attempt=UTC_TIMESTAMP(3) WHERE repository=? AND id=?")) {
            sql.setString(1, config.repository);
            sql.setBytes(2, bytes(first.id()));
            sql.executeUpdate();
        }
        var retried = database.pending(List.of(firstToken)).get(0);
        assertEquals(first.id(), retried.mail().id());
        assertEquals(1, retried.attempts());
        database.acknowledge(retried);
        assertEquals(second.id(), database.pending(List.of(firstToken)).get(0).mail().id());
    }

    @Test
    void exportReadsPersistedDataAndPendingMailWithOriginalIds() throws Exception {
        UUID token = UUID.randomUUID();
        var lease = database.claim(player, token);
        database.save(player, token, lease.epoch(), new byte[]{ 0, -1 }, true);
        var mail = new Mail(UUID.randomUUID(), player, "hello");
        database.admit(mail);
        assertArrayEquals(new byte[]{ 0, -1 }, database.exportData(null).entries().get(0).payload());
        assertEquals(mail.id(), database.exportMails(0).entries().get(0).messageId());
    }

    @Test
    void storageInstancesFenceStaleWritesAndPermitHandoff() throws Exception {
        try (StorageHandle first = storage("one"); StorageHandle second = storage("two")) {
            assertTrue(acquire(first.value));
            assertFalse(acquire(second.value));
            var lost = new CountDownLatch(1);
            first.value.setLockLostHandler((key, version) -> lost.countDown());
            expire(player);
            assertTrue(acquire(second.value));
            second.value.pushSnapshot(player, new byte[]{ 2 });
            second.value.unlock(player);
            await(() -> Arrays.equals(new byte[]{ 2 }, readSnapshot()));
            first.value.pushSnapshot(player, new byte[]{ 9 });
            assertTrue(lost.await(5, TimeUnit.SECONDS));
            assertFalse(first.value.isLocked(player));
            assertArrayEquals(new byte[]{ 2 }, database.snapshot(player));
        }
    }

    @Test
    void storageMailboxRetriesHandlerAndSurvivesProducerRestart() throws Exception {
        Path producerPath = directory.resolve("producer");
        JdbcPlayerDataStorage producer = new JdbcPlayerDataStorage(config, producerPath, true, true);
        producer.pushMail(player, "restart reward");
        producer.close();
        try (StorageHandle restarted = new StorageHandle(new JdbcPlayerDataStorage(config, producerPath, true, true));
            StorageHandle recipient = storage("recipient")) {
            restarted.value.initialized().get(5, TimeUnit.SECONDS);
            assertTrue(acquire(recipient.value));
            var ids = new CopyOnWriteArrayList<UUID>();
            var delivered = new CountDownLatch(1);
            recipient.value.setMailHandler(mail -> {
                ids.add(mail.id());
                if (ids.size() == 1) throw new IllegalStateException("temporary business failure");
                delivered.countDown();
            });
            recipient.value.doMailsLoad(player);
            assertTrue(delivered.await(10, TimeUnit.SECONDS));
            assertEquals(ids.get(0), ids.get(1));
            await(() -> {
                try {
                    return database.exportMails(0).entries().isEmpty();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Test
    void importRefusesLiveLeasesAndRetainsExistingMailIds() throws Exception {
        try (StorageHandle storage = storage("import")) {
            UUID token = UUID.randomUUID();
            database.claim(player, token);
            var data = new TransferEntry(TransferEntry.Kind.DATA, player, new byte[]{ 3 }, null);
            assertEquals(TransferResult.Status.LOCKED, storage.value.importEntry(data).get().status());
            expire(player);
            assertEquals(TransferResult.Status.SUCCESS, storage.value.importEntry(data).get().status());
            assertArrayEquals(new byte[]{ 3 }, database.snapshot(player));
            var mail = new TransferEntry(TransferEntry.Kind.MAIL, player, "mail".getBytes(StandardCharsets.UTF_8), UUID.randomUUID());
            storage.value.importEntry(mail).get();
            storage.value.importEntry(mail).get();
            var exported = storage.value.exportSources().get(0).next(TransferEntry.Kind.MAIL).get();
            assertEquals(1, exported.entries().size());
            assertEquals(mail.messageId(), exported.entries().get(0).messageId());
        }
    }

    private StorageHandle storage(String name) throws Exception {
        var result = new StorageHandle(new JdbcPlayerDataStorage(config, directory.resolve(name), true, true));
        result.value.initialized().get(5, TimeUnit.SECONDS);
        return result;
    }

    private boolean acquire(JdbcPlayerDataStorage storage) throws Exception {
        var result = new CompletableFuture<Boolean>();
        storage.lockAndLoadData(player, (okay, bytes) -> result.complete(okay));
        return result.get(5, TimeUnit.SECONDS);
    }

    private void expire(UUID key) throws Exception {
        try (var connection = data.getConnection(); var sql = connection.prepareStatement("UPDATE " + SqlLeaseRepository.PLAYERS
                + " SET lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE repository=? AND player_id=?")) {
            sql.setString(1, config.repository);
            sql.setBytes(2, bytes(key));
            sql.executeUpdate();
        }
    }

    private byte[] readSnapshot() {
        try {
            return database.snapshot(player);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] bytes(UUID key) {
        return ByteBuffer.allocate(16).putLong(key.getMostSignificantBits()).putLong(key.getLeastSignificantBits()).array();
    }

    private static void await(BooleanSupplier check) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!check.getAsBoolean()) {
            if (System.nanoTime() > until) fail("SQL condition did not become true");
            Thread.sleep(20);
        }
    }

    private record StorageHandle(JdbcPlayerDataStorage value) implements AutoCloseable {
        public void close() {
            value.close();
        }
    }
}
