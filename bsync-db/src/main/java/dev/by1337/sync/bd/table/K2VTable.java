package dev.by1337.sync.bd.table;

import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

public interface K2VTable<K, V> {

    default void putAll(Collection<K2VPair<K, V>> queue) throws SQLException {
        putAll(new LinkedList<>(queue), Integer.MAX_VALUE, v -> {});
    }

    default void putAll(Queue<K2VPair<K, V>> queue, int limit) throws SQLException {
        putAll(queue, limit, c -> {});
    }

    void putAll(Queue<K2VPair<K, V>> queue, int limit, Consumer<K2VPair<K, V>> c) throws SQLException;

    default void removeAll(Collection<K> queue) throws SQLException {
        removeAll(new LinkedList<>(queue), Integer.MAX_VALUE);
    }

    default void removeAll(Queue<K> queue, int limit) throws SQLException {
        removeAll(queue, limit, p -> {});
    }

    void removeAll(Queue<K> queue, int limit, Consumer<K> c) throws SQLException;

    void put(@NotNull K key, @NotNull V value) throws SQLException;

    @NotNull Optional<V> get(@NotNull K key) throws SQLException;

    boolean remove(@NotNull K key) throws SQLException;

    boolean contains(@NotNull K key) throws SQLException;

    void close() throws SQLException;
}
