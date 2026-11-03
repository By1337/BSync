package dev.by1337.sync.server.channel.handler.lock;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.bd.repo.UUID2MediumBLOBRepository;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import dev.by1337.sync.common.transfer.*;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.database.table.BatchedK2VCache;
import org.junit.jupiter.api.*;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class RepositoryTransferStorageTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("transfer-server-test");
    private static final EventLoopWorker IO = new EventLoopWorker("transfer-io-test");
    private HikariDataSource source;
    private UUID2MediumBLOBRepository database;
    private BatchedK2VCache<UUID, byte[]> cache;
    private ServerLockerHandler locks;
    private ServerBlobRepoHandler blobs;
    private final SocketConnection owner = message -> {
    };
    private final Connection connection = new Connection() {
        public void write(ChannelMessage message) {
        }

        public SocketConnection transport() {
            return owner;
        }
    };

    @BeforeEach
    void setUp() throws Exception {
        source = new HikariDataSource();
        source.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL");
        source.setMaximumPoolSize(2);
        database = new UUID2MediumBLOBRepository(source, "test_snapshots");
        run(WORKER, () -> {
            locks = new ServerLockerHandler();
            blobs = new ServerBlobRepoHandler();
            cache = new BatchedK2VCache<>(database, IO);
            set(blobs, "persistent", database);
            set(blobs, "blobRepository", cache);
            set(blobs, "locks", locks);
            set(blobs, "io", IO);
            set(blobs, "eventLoop", WORKER);
            set(blobs, "log", LoggerFactory.getLogger(RepositoryTransferStorageTest.class));
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        run(IO, cache::close);
        source.close();
    }

    @Test
    void sqlKeysetExportIncludesUncachedZeroAndUnsignedUuidsAcrossPages() throws Exception {
        Set<UUID> expected = new HashSet<>();
        for (int i = 0; i < 600; i++) {
            UUID key = i == 0 ? new UUID(0, 0) : UUID.randomUUID();
            expected.add(key);
            database.put(key, new byte[]{ (byte) i });
        }
        Set<UUID> exported = new HashSet<>();
        UUID cursor = null;
        while (true) {
            var page = export(cursor);
            assertTrue(page.error().isEmpty());
            assertTrue(page.entries().size() <= 256);
            for (var entry : page.entries()) assertTrue(exported.add(entry.key()));
            if (page.done()) break;
            assertNotEquals(cursor, page.afterKey());
            cursor = page.afterKey();
        }
        assertEquals(expected, exported);
    }

    @Test
    void byteBudgetDoesNotDropNextLargeRecord() throws Exception {
        byte[] large = new byte[3 << 20];
        Arrays.fill(large, (byte) 7);
        UUID first = new UUID(0, 1), second = new UUID(0, 2);
        database.put(first, large);
        database.put(second, large);
        var page = export(null);
        assertEquals(1, page.entries().size());
        assertFalse(page.done());
        assertEquals(first, page.entries().get(0).key());
        var next = export(page.afterKey());
        assertEquals(second, next.entries().get(0).key());
        assertArrayEquals(large, next.entries().get(0).payload());
    }

    @Test
    void lockedPlayerIsSkippedAndExistingSnapshotIsPreserved() throws Exception {
        UUID key = UUID.randomUUID();
        database.put(key, new byte[]{ 1 });
        run(WORKER, () -> lockMap().tryLock(key, message -> {
        }));
        var result = importData(key, new byte[]{ 2 }).get(5, TimeUnit.SECONDS);
        assertEquals(TransferResult.Status.LOCKED, result.status());
        assertArrayEquals(new byte[]{ 1 }, database.get(key).orElseThrow());
    }

    @Test
    void committedImportFlushesOlderQueuedSnapshotAndRefreshesReadCache() throws Exception {
        UUID key = UUID.randomUUID();
        run(WORKER, () -> cache.put(key, new byte[]{ 1 }));
        assertEquals(TransferResult.Status.SUCCESS, importData(key, new byte[]{ 2 }).get(5, TimeUnit.SECONDS).status());
        run(IO, cache::flushPending);
        assertArrayEquals(new byte[]{ 2 }, database.get(key).orElseThrow());
        assertArrayEquals(new byte[]{ 2 }, blobs.loadBlob(key));
        assertNull(locks.getLock(key));
    }

    @Test
    void importReservationCannotExpireOrBeReclaimedDuringSlowCommit() throws Exception {
        UUID key = UUID.randomUUID();
        var blocked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        IO.executeReliably(() -> {
            blocked.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(blocked.await(5, TimeUnit.SECONDS));
        CompletableFuture<S2CImportResultPacket> result = importData(key, new byte[]{ 3 });
        try {
            run(WORKER, () -> {
                var lock = locks.getLock(key);
                assertNotNull(lock);
                assertTrue(lock.importing);
                lock.lastConfirm = 0;
                lock.disconnected = true;
                lock.disconnectedAt = 0;
                assertNull(lockMap().tryLock(key, message -> {
                }));
                var tick = ServerLockerHandler.class.getDeclaredMethod("tick");
                tick.setAccessible(true);
                tick.invoke(locks);
                assertSame(lock, locks.getLock(key));
            });
            assertFalse(result.isDone());
        } finally {
            release.countDown();
        }
        assertEquals(TransferResult.Status.SUCCESS, result.get(5, TimeUnit.SECONDS).status());
        assertNull(locks.getLock(key));
    }

    @Test
    void sharedDatabasePartitionsHaveNoDuplicatesAndCoverEverySnapshot() throws Exception {
        Set<UUID> expected = new HashSet<>();
        expected.add(new UUID(Long.MIN_VALUE, 0));
        expected.add(new UUID(0, 0));
        var random = new Random(7331);
        for (int i = 0; i < 600; i++) expected.add(new UUID(random.nextLong(), random.nextLong()));
        for (UUID key : expected) database.put(key, new byte[]{ 1 });
        Set<UUID> exported = new HashSet<>();
        for (int index = 0; index < 3; index++) {
            var shard = new ShardSelection(3, index);
            UUID cursor = null;
            while (true) {
                var page = export(cursor, shard);
                for (var entry : page.entries()) {
                    assertTrue(shard.includes(entry.key()));
                    assertTrue(exported.add(entry.key()), "Shared DB row must not appear on two shards");
                }
                if (page.done()) break;
                assertNotEquals(cursor, page.afterKey());
                cursor = page.afterKey();
            }
        }
        assertEquals(expected, exported);
    }

    @Test
    void filteredOutFullPageAdvancesCursorInsteadOfEndingExport() throws Exception {
        for (int i = 0; i < 257; i++) database.put(new UUID(0, 2L * i + 1), new byte[]{ 1 });
        UUID matching = new UUID(0, 1000);
        database.put(matching, new byte[]{ 2 });
        var shard = new ShardSelection(2, 0);
        var first = export(null, shard);
        assertTrue(first.entries().isEmpty());
        assertFalse(first.done());
        assertNotNull(first.afterKey());
        var second = export(first.afterKey(), shard);
        assertTrue(second.done());
        assertEquals(1, second.entries().size());
        assertEquals(matching, second.entries().get(0).key());
    }

    @Test
    void byteBoundaryAfterSkippedRowsDoesNotSkipTheNextMatchingSnapshot() throws Exception {
        byte[] large = new byte[3 << 20];
        for (int i = 1; i <= 5; i++) database.put(new UUID(0, i), (i & 1) == 0 ? large : new byte[]{ 1 });
        var shard = new ShardSelection(2, 0);
        var first = export(null, shard);
        assertEquals(new UUID(0, 2), first.entries().get(0).key());
        assertEquals(new UUID(0, 3), first.afterKey());
        assertFalse(first.done());
        var second = export(first.afterKey(), shard);
        assertEquals(1, second.entries().size());
        assertEquals(new UUID(0, 4), second.entries().get(0).key());
        assertTrue(second.done());
    }

    private S2CExportPagePacket export(UUID cursor) throws Exception {
        return export(cursor, ShardSelection.ALL);
    }

    private S2CExportPagePacket export(UUID cursor, ShardSelection shard) throws Exception {
        var result = new CompletableFuture<S2CExportPagePacket>();
        run(WORKER, () -> blobs.exportPage(new C2SExportPagePacket(TransferEntry.Kind.DATA, cursor, 0,
                shard.count(), shard.index())).then(result::complete));
        return result.get(5, TimeUnit.SECONDS);
    }

    private CompletableFuture<S2CImportResultPacket> importData(UUID key, byte[] value) throws Exception {
        var result = new CompletableFuture<S2CImportResultPacket>();
        run(WORKER, () -> blobs.importData(new C2SImportDataPacket(key, value), connection).then(result::complete));
        return result;
    }

    private ServerLockerHandler.LockMap lockMap() throws Exception {
        return (ServerLockerHandler.LockMap) get(locks, "lockMap");
    }

    private static void set(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object get(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void run(EventLoopWorker worker, CheckedRunnable work) throws Exception {
        var result = new CompletableFuture<Void>();
        worker.executeReliably(() -> {
            try {
                work.run();
                result.complete(null);
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        result.get(5, TimeUnit.SECONDS);
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
