package dev.by1337.sync.bd.table;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

public class K2VCache<K, V> implements K2VTable<K, V> {
    private static final Logger log = LoggerFactory.getLogger(K2VCache.class);
    private final K2VTable<K, V> table;
    private final Cache<K, V> cache;

    public K2VCache(K2VTable<K, V> table) {
        this(table, b -> b
                .maximumSize(100_000)
                .expireAfterAccess(Duration.ofMinutes(5))
        );
    }

    public K2VCache(K2VTable<K, V> table, Consumer<Caffeine<K, V>> c) {
        this.table = table;
        Caffeine<K, V> b = (Caffeine<K, V>) Caffeine.newBuilder();
        c.accept(b);
        cache = b.build();

    }

    @Override
    public void putAll(Queue<K2VPair<K, V>> queue, int limit, Consumer<K2VPair<K, V>> c) throws SQLException {
        Consumer<K2VPair<K, V>> c1 = c.andThen(p -> cache.put(p.key, p.value));
        table.putAll(queue, limit, c1);
    }

    @Override
    public void put(@NotNull K key, @NotNull V value) throws SQLException {
        table.put(key, value);
        cache.put(key, value);
    }

    @Override
    public @NotNull Optional<V> get(@NotNull K key) throws SQLException {
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
    public void removeAll(Queue<K> queue, int limit, Consumer<K> c) throws SQLException {
        Consumer<K> c1 = c.andThen(cache::invalidate);
        table.removeAll(queue, limit, c1);
    }

    @Override
    public boolean remove(@NotNull K key) throws SQLException {
        cache.invalidate(key);
        return table.remove(key);
    }

    @Override
    public boolean contains(@NotNull K key) throws SQLException {
        return cache.getIfPresent(key) != null || table.contains(key);
    }

    public K2VTable<K, V> table() {
        return table;
    }

    public Cache<K, V> cache() {
        return cache;
    }

    @Override
    public void close() throws SQLException {
        cache.invalidateAll();
        table.close();
    }
}
