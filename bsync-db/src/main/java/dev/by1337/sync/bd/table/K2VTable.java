package dev.by1337.sync.bd.table;

import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.Optional;

public interface K2VTable<K, V> {
    void put(@NotNull K key, @NotNull V value) throws SQLException;

    @NotNull Optional<V> get(@NotNull K key) throws SQLException;

    boolean delete(@NotNull K key) throws SQLException;

    boolean contains(@NotNull K key) throws SQLException;
    void close() throws SQLException;
}
