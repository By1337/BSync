package dev.by1337.sync.server.database.table;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.by1337.sync.bd.table.K2VPair;
import dev.by1337.sync.bd.table.K2VTable;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

public class BatchedK2VCache<K, V> implements K2VTable<K, V> {
    private static final Logger log = LoggerFactory.getLogger(BatchedK2VCache.class);
    private final K2VTable<K, V> base;
    private final DataBatcher<K> removeBatcher;
    private final DataBatcher<K2VPair<K, V>> addBatcher;

    private final Cache<K, V> cache;

    public BatchedK2VCache(K2VTable<K, V> base, EventLoopWorker worker) {
        this(base, worker, b -> b
                .maximumSize(100_000)
                .expireAfterAccess(Duration.ofMinutes(5))
        );
    }

    public BatchedK2VCache(K2VTable<K, V> base, EventLoopWorker worker, Consumer<Caffeine<K, V>> c) {
        this.base = base;
        addBatcher = new DataBatcher<>(4096, base::putAll, worker);
        removeBatcher = new DataBatcher<>(4096, base::removeAll, worker);
        Caffeine<K, V> b = (Caffeine<K, V>) Caffeine.newBuilder();
        c.accept(b);
        cache = b.build();
    }


    @Override
    public void putAll(Queue<K2VPair<K, V>> queue, int limit, Consumer<K2VPair<K, V>> c) {
        K2VPair<K, V> v;
        while (limit-- > 0 && (v = queue.poll()) != null) {
            c.accept(v);
            addBatcher.offer(v);
            cache.put(v.key, v.value);
        }
    }

    @Override
    public void removeAll(Queue<K> queue, int limit, Consumer<K> c) {
        K v;
        while (limit-- > 0 && (v = queue.poll()) != null) {
            c.accept(v);
            removeBatcher.offer(v);
            cache.invalidate(v);
        }
    }

    @Override
    public void put(@NonNull K key, @NonNull V value) {
        addBatcher.offer(new K2VPair<>(key, value));
        cache.put(key, value);
    }

    @Override
    public @NotNull Optional<V> get(@NonNull K key) {
        return Optional.ofNullable(cache.get(key, k -> {
            try {
                return base.get(k).orElse(null);
            } catch (SQLException e) {
                log.error("Failed to get {} {}", key, k, e);
            }
            return null;
        }));
    }

    @Override
    public boolean remove(@NonNull K key) {
        removeBatcher.offer(key);
        cache.invalidate(key);
        return true;
    }

    @Override
    public boolean contains(@NonNull K key) throws SQLException {
        return cache.getIfPresent(key) != null || base.contains(key);
    }

    @Override
    public void close() throws SQLException {
        addBatcher.close();
        removeBatcher.close();
        base.close();
    }
}
