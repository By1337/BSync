package dev.by1337.sync.server.database;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.by1337.sync.server.database.table.MailboxRepository;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public class CachedMailbox {
    private final MailboxRepository mailbox;
    private final Cache<UUID, List<MailboxRepository.Message>> cache;

    public CachedMailbox(MailboxRepository mailbox) {
        this.mailbox = mailbox;
        cache = Caffeine.newBuilder()
                .maximumSize(100_000)
                .expireAfterAccess(Duration.ofMinutes(5))
                .build();
    }
    public List<MailboxRepository.Message> getAll(UUID owner, long afterId) throws SQLException {
        //return cache.get(owner)
        return null;
    }
}
