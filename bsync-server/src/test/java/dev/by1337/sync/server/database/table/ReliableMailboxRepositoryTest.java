package dev.by1337.sync.server.database.table;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.transfer.ShardSelection;
import org.junit.jupiter.api.*;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class ReliableMailboxRepositoryTest {
    private HikariDataSource source;
    private ReliableMailboxRepository repository;
    private final UUID owner = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {
        source = new HikariDataSource();
        source.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL");
        source.setMaximumPoolSize(1);
        repository = new ReliableMailboxRepository(source, "reliable_mailbox");
    }

    @AfterEach
    void close() {
        source.close();
    }

    @Test
    void deduplicationSurvivesRepositoryRestartAndAcknowledgement() throws Exception {
        var mail = new Mail(UUID.randomUUID(), owner, "reward");
        assertTrue(repository.put(mail));
        assertTrue(repository.put(mail));
        assertEquals(mail, repository.next(owner).mail());
        repository.acknowledge(mail.id());
        repository = new ReliableMailboxRepository(source, "reliable_mailbox");
        assertTrue(repository.put(mail));
        assertNull(repository.next(owner));
    }

    @Test
    void conflictingIdIsRejectedEvenAfterDelivery() throws Exception {
        var mail = new Mail(UUID.randomUUID(), owner, "original");
        assertTrue(repository.put(mail));
        assertFalse(repository.put(new Mail(mail.id(), owner, "different")));
        assertFalse(repository.put(new Mail(mail.id(), UUID.randomUUID(), "original")));
        repository.acknowledge(mail.id());
        assertFalse(repository.put(new Mail(mail.id(), owner, "different")));
        assertNull(repository.next(owner));
    }

    @Test
    void failedDeliveryRetainsOriginalMailAndOrderAcrossRestart() throws Exception {
        var first = new Mail(UUID.randomUUID(), owner, "first");
        var second = new Mail(UUID.randomUUID(), owner, "second");
        repository.put(first);
        repository.put(second);
        long retryAt = System.currentTimeMillis() + 60_000;
        repository.failed(first.id(), "handler exception", retryAt);
        repository = new ReliableMailboxRepository(source, "reliable_mailbox");
        var next = repository.next(owner);
        assertEquals(first, next.mail());
        assertEquals(1, next.attempts());
        assertEquals(retryAt, next.retryAt());
        repository.acknowledge(first.id());
        assertEquals(second, repository.next(owner).mail());
    }

    @Test
    void samePayloadWithDifferentIdsIsNotDeduplicated() throws Exception {
        var first = new Mail(UUID.randomUUID(), owner, "same");
        var second = new Mail(UUID.randomUUID(), owner, "same");
        repository.put(first);
        repository.put(second);
        repository.acknowledge(first.id());
        assertEquals(second, repository.next(owner).mail());
    }

    @Test
    void migratesLegacyMailboxWithoutLosingOrResurrectingMessages() throws Exception {
        var legacy = new MailboxRepository(source, "legacy_mailbox");
        legacy.put(1, owner, "old first");
        legacy.put(2, owner, "old second");
        repository.migrateLegacy("legacy_mailbox");
        var first = repository.next(owner).mail();
        assertEquals("old first", first.payload());
        assertTrue(legacy.loadAll(owner).isEmpty());
        repository.acknowledge(first.id());
        // Simulate a crash between committed migration and legacy deletion.
        legacy.put(1, owner, "old first");
        repository.migrateLegacy("legacy_mailbox");
        assertEquals("old second", repository.next(owner).mail().payload());
    }

    @Test
    void exportPagesExcludeReceiptsAndPreserveIdsAtByteBoundary() throws Exception {
        var delivered = new Mail(UUID.randomUUID(), owner, "already delivered");
        repository.put(delivered);
        repository.acknowledge(delivered.id());
        var ids = new java.util.ArrayList<UUID>();
        String payload = "m".repeat((1 << 20) - 1);
        for (int i = 0; i < 5; i++) {
            var mail = new Mail(UUID.randomUUID(), owner, payload);
            ids.add(mail.id());
            repository.put(mail);
        }
        var first = repository.exportPage(0);
        assertEquals(4, first.entries().size());
        assertFalse(first.done());
        var second = repository.exportPage(first.afterSequence());
        assertEquals(1, second.entries().size());
        assertTrue(second.done());
        var exported = new java.util.ArrayList<UUID>();
        first.entries().forEach(mail -> exported.add(mail.id()));
        second.entries().forEach(mail -> exported.add(mail.id()));
        assertEquals(ids, exported);
    }

    @Test
    void sharedMailboxIsPartitionedByRecipientWithoutDeduplicatingEqualPayloads() throws Exception {
        var expected = new java.util.HashSet<UUID>();
        for (UUID player : java.util.List.of(new UUID(Long.MIN_VALUE, 0), new UUID(-1, 0), new UUID(0, 0), new UUID(0, 1))) {
            for (int i = 0; i < 2; i++) {
                var mail = new Mail(UUID.randomUUID(), player, "same payload");
                repository.put(mail);
                expected.add(mail.id());
            }
        }
        var exported = new java.util.HashSet<UUID>();
        for (int index = 0; index < 3; index++) {
            var shard = new ShardSelection(3, index);
            long cursor = 0;
            while (true) {
                var page = repository.exportPage(cursor, shard::includes);
                for (Mail mail : page.entries()) {
                    assertTrue(shard.includes(mail.owner()));
                    assertTrue(exported.add(mail.id()));
                }
                if (page.done()) break;
                assertTrue(page.afterSequence() > cursor);
                cursor = page.afterSequence();
            }
        }
        assertEquals(expected, exported);
    }

    @Test
    void mailboxCursorAdvancesAcrossAnEntirePageOfOtherShards() throws Exception {
        UUID otherShard = new UUID(0, 1), thisShard = new UUID(0, 2);
        for (int i = 0; i < 256; i++) repository.put(new Mail(UUID.randomUUID(), otherShard, "other"));
        var matching = new Mail(UUID.randomUUID(), thisShard, "this");
        repository.put(matching);
        var shard = new ShardSelection(2, 0);
        var first = repository.exportPage(0, shard::includes);
        assertTrue(first.entries().isEmpty());
        assertFalse(first.done());
        assertTrue(first.afterSequence() > 0);
        var second = repository.exportPage(first.afterSequence(), shard::includes);
        assertEquals(java.util.List.of(matching), second.entries());
        assertTrue(second.done());
    }

}
