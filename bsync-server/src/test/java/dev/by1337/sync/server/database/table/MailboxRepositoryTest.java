package dev.by1337.sync.server.database.table;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MailboxRepositoryTest {
    private HikariDataSource dataSource;
    private MailboxRepository repository;
    private final UUID player = new UUID(Long.MIN_VALUE, Long.MAX_VALUE);
    private final UUID other = new UUID(13, 37);

    @BeforeEach
    void setUp() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL");
        dataSource.setMaximumPoolSize(1);
        repository = new MailboxRepository(dataSource, "test_mailbox");
    }

    @AfterEach
    void tearDown() {
        if (dataSource != null) dataSource.close();
    }

    @Test
    void emptyMailboxHasNoMessagesAndZeroMaxId() throws SQLException {
        assertEquals(List.of(), repository.loadAll(player));
        assertEquals(0, repository.getMaxId());
    }

    @Test
    void messagesAreOrderedAndIsolatedByOwner() throws SQLException {
        var first = new MailboxRepository.Mail(1, player, "Hi");
        var second = new MailboxRepository.Mail(2, player, "line one\nline two");
        repository.put(second);
        repository.put(first);
        repository.put(1, other, "other");
        assertEquals(List.of(first, second), repository.loadAll(player));
        assertEquals(List.of(new MailboxRepository.Mail(1, other, "other")), repository.loadAll(other));
        assertEquals(2, repository.getMaxId());
    }

    @Test
    void deletionMatchesBothOwnerAndIdAndIsIdempotent() throws SQLException {
        var mail = new MailboxRepository.Mail(1, player, "first");
        repository.put(mail);
        repository.put(1, other, "other");
        repository.remove(mail);
        repository.remove(mail);
        assertEquals(List.of(), repository.loadAll(player));
        assertEquals(1, repository.loadAll(other).size());
    }

    @Test
    void duplicateOwnerAndIdIsRejectedWithoutReplacingPayload() throws SQLException {
        repository.put(1, player, "original");
        assertThrows(SQLException.class, () -> repository.put(1, player, "replacement"));
        assertEquals(List.of(new MailboxRepository.Mail(1, player, "original")), repository.loadAll(player));
    }

    @Test
    void batchInsertHonorsLimitAndReportsConsumedMessages() throws SQLException {
        var first = new MailboxRepository.Mail(1, player, "first");
        var second = new MailboxRepository.Mail(2, player, "second");
        var third = new MailboxRepository.Mail(3, player, "third");
        var queue = new ArrayDeque<>(List.of(first, second, third));
        List<MailboxRepository.Mail> consumed = new ArrayList<>();
        repository.putAll(queue, 2, consumed::add);
        assertEquals(List.of(first, second), consumed);
        assertEquals(List.of(third), new ArrayList<>(queue));
        assertEquals(List.of(first, second), repository.loadAll(player));
        repository.putAll(queue, Integer.MAX_VALUE, consumed::add);
        assertTrue(queue.isEmpty());
        assertEquals(List.of(first, second, third), repository.loadAll(player));
    }

    @Test
    void batchRemoveHonorsLimitAndPreservesOtherOwners() throws SQLException {
        var first = new MailboxRepository.Mail(1, player, "first");
        var second = new MailboxRepository.Mail(2, player, "second");
        repository.put(first);
        repository.put(second);
        repository.put(1, other, "other");
        var queue = new ArrayDeque<>(List.of(first, second));
        List<MailboxRepository.Mail> consumed = new ArrayList<>();
        repository.removeAll(queue, 1, consumed::add);
        assertEquals(List.of(first), consumed);
        assertEquals(List.of(second), new ArrayList<>(queue));
        assertEquals(List.of(second), repository.loadAll(player));
        repository.removeAll(queue, Integer.MAX_VALUE, consumed::add);
        assertTrue(queue.isEmpty());
        assertEquals(List.of(first, second), consumed);
        assertEquals(List.of(), repository.loadAll(player));
        assertEquals(1, repository.loadAll(other).size());
    }
}
