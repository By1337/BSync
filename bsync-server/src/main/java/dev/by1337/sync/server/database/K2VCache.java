package dev.by1337.sync.server.database;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.by1337.sync.server.database.table.K2VTable;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

public class K2VCache<K, V> implements K2VTable<K, V> {
    private static final Logger log = LoggerFactory.getLogger(K2VCache.class);
    private final K2VTable<K, V> table;
    private final Cache<K, V> cache;

    public K2VCache(K2VTable<K, V> table) {
        this.table = table;
        cache = Caffeine.<K, V>newBuilder()
                .maximumSize(100_000)
                .expireAfterAccess(Duration.ofMinutes(5))
                .build();
    }

    @Override
    public void put(@NonNull K key, @NonNull V value) throws SQLException {
        cache.put(key, value);
        table.put(key, value);
    }

    @Override
    public @NotNull Optional<V> get(@NonNull K key) throws SQLException {
        return Optional.ofNullable(cache.get(key, k -> {
            try {
                return table.get(k).orElse(null);
            } catch (SQLException e) {
                log.error("Failed to get {} {}", key, k, e);
            }
            return null;
        }));
    }

    @Override
    public boolean delete(@NonNull K key) throws SQLException {
        cache.invalidate(key);
        return table.delete(key);
    }

    @Override
    public boolean contains(@NonNull K key) throws SQLException {
        return cache.getIfPresent(key) != null || table.contains(key);
    }

    @Override
    public void close() throws SQLException {
        cache.invalidateAll();
        table.close();
    }
}
