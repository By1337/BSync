package dev.by1337.sync.k2v.storage.sql;

import dev.by1337.sync.common.mail.MailJournal;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class JdbcLeaseScheduleTest {
    @TempDir
    Path directory;
    private SqlLeaseRepository database;
    private JdbcPlayerDataStorage storage;
    private final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    private final UUID player = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {
        database = mock(SqlLeaseRepository.class);
        when(database.claim(any(), any())).thenReturn(new SqlLeaseRepository.Claim(1, new byte[]{ 1 }, 30_000));
        when(database.save(any(), any(), anyLong(), any(), anyBoolean())).thenReturn(true);
        when(database.renew(any(), any(), anyLong())).thenReturn(true);
        storage = new JdbcPlayerDataStorage(SqlStorageConfig.parse("mysql://localhost/db/repo?user=u"), database,
                new MailJournal(null), directory, true, false, clock::get);
        storage.initialized().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void close() {
        storage.close();
    }

    private void acquire() throws Exception {
        var loaded = new CompletableFuture<Boolean>();
        storage.lockAndLoadData(player, (success, data) -> loaded.complete(success));
        assertTrue(loaded.get(5, TimeUnit.SECONDS));
    }

    @Test
    void sqliteKeepsLocalOwnershipWithoutExpiryOrDatabaseRenewals() throws Exception {
        storage.close();
        storage = new JdbcPlayerDataStorage(SqlStorageConfig.parse("sqlite://" + directory.resolve("local.db").toUri().getRawPath()),
                database, new MailJournal(null), directory, true, false, clock::get);
        storage.initialized().get(5, TimeUnit.SECONDS);
        acquire();
        clock.addAndGet(TimeUnit.HOURS.toNanos(1));
        storage.maintenance();
        assertTrue(storage.isLocked(player));
        verify(database, never()).renew(any(), any(), anyLong());
        storage.pushSnapshot(player, new byte[]{ 5 });
        storage.unlock(player);
        verify(database, timeout(5000)).save(eq(player), any(), eq(1L), org.mockito.AdditionalMatchers.aryEq(new byte[]{ 5 }), eq(true));
    }

    @Test
    void leavesDatabaseAloneForAlmostTheWholeLease() throws Exception {
        acquire();
        for (int i = 0; i < 10; i++) {
            storage.maintenance();
            assertTrue(storage.isLocked(player));
        }
        verify(database, never()).renew(any(), any(), anyLong());
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(26_999));
        storage.maintenance();
        verify(database, never()).renew(any(), any(), anyLong());
        clock.incrementAndGet(); // still one nanosecond, not one millisecond
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
        storage.maintenance();
        verify(database, timeout(5000).times(1)).renew(eq(player), any(), eq(1L));
        for (int i = 0; i < 10; i++) storage.maintenance();
        verify(database, times(1)).renew(eq(player), any(), eq(1L));
        verify(database, never()).snapshot(any());
    }

    @Test
    void localDeadlineInvalidatesOwnershipEvenWhenRenewalCannotFinish() throws Exception {
        acquire();
        var lost = new CountDownLatch(1);
        storage.setLockLostHandler((key, version) -> lost.countDown());
        clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
        assertFalse(storage.isLocked(player));
        storage.maintenance();
        assertTrue(lost.await(5, TimeUnit.SECONDS));
        verify(database, never()).renew(any(), any(), anyLong());
        assertThrows(IllegalStateException.class, () -> storage.pushSnapshot(player, new byte[]{ 2 }));
    }

    @Test
    void renewalRejectionDoesNotReacquireOrResurrectTheLease() throws Exception {
        acquire();
        var lost = new CountDownLatch(1);
        storage.setLockLostHandler((key, version) -> lost.countDown());
        when(database.renew(any(), any(), anyLong())).thenReturn(false);
        clock.addAndGet(TimeUnit.SECONDS.toNanos(27));
        storage.maintenance();
        assertTrue(lost.await(5, TimeUnit.SECONDS));
        assertFalse(storage.isLocked(player));
        verify(database, times(1)).claim(any(), any());
        verify(database, never()).save(any(), any(), anyLong(), any(), anyBoolean());
    }

    @Test
    void savesAndReleasesTheFinalSnapshotInOneFencedMutation() throws Exception {
        acquire();
        storage.pushSnapshot(player, new byte[]{ 9 });
        storage.unlock(player);
        verify(database, timeout(5000)).save(eq(player), any(), eq(1L), org.mockito.AdditionalMatchers.aryEq(new byte[]{ 9 }), eq(true));
    }

    @Test
    void lateSuccessfulRenewalCannotRestoreExpiredLocalOwnership() throws Exception {
        acquire();
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var lost = new CountDownLatch(1);
        storage.setLockLostHandler((key, version) -> lost.countDown());
        when(database.renew(any(), any(), anyLong())).thenAnswer(call -> {
            entered.countDown();
            assertTrue(resume.await(5, TimeUnit.SECONDS));
            return true;
        });
        try {
            clock.addAndGet(TimeUnit.SECONDS.toNanos(27));
            storage.maintenance();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            clock.addAndGet(TimeUnit.SECONDS.toNanos(4));
            storage.maintenance();
            assertTrue(lost.await(5, TimeUnit.SECONDS));
        } finally {
            resume.countDown();
        }
        verify(database, timeout(5000)).save(eq(player), any(), eq(1L), isNull(), eq(true));
        assertFalse(storage.isLocked(player));
        verify(database, times(1)).claim(any(), any());
    }
}
