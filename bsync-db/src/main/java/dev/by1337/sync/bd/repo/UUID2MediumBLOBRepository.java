package dev.by1337.sync.bd.repo;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.bd.table.K2VPair;
import dev.by1337.sync.bd.table.K2VTable;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;

public final class UUID2MediumBLOBRepository implements K2VTable<UUID, byte[]> {
    private final HikariDataSource dataSource;
    private final String tableName;

    public UUID2MediumBLOBRepository(HikariDataSource dataSource, String tableName) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        try {
            createTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable() throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS `%s` (
                    `id` BINARY(16) NOT NULL,
                    `updated_at` TIMESTAMP NOT NULL
                        DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    `data` MEDIUMBLOB NOT NULL,
                
                    PRIMARY KEY (`id`)
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
        }
    }

    public void putAll(Queue<K2VPair<UUID, byte[]>> queue, int limit, Consumer<K2VPair<UUID, byte[]>> c) throws SQLException {
        if (queue.isEmpty()) return;
        String sql = """
                INSERT INTO `%s` (`id`, `data`)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE
                    `data` = VALUES(`data`)
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);

            K2VPair<UUID, byte @NotNull []> p;
            while (limit-- > 0 && (p = queue.poll()) != null){
                statement.setBytes(1, UUIDUtil.uuidToBytes(p.key));
                statement.setBytes(2, p.value);
                c.accept(p);
                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
        }
    }

    public void put(@NotNull UUID uuid, byte @NotNull [] data) throws SQLException {
        String sql = """
                INSERT INTO `%s` (`id`, `data`)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE
                    `data` = VALUES(`data`)
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, UUIDUtil.uuidToBytes(uuid));
            statement.setBytes(2, data);

            statement.executeUpdate();
        }
    }

    public @NotNull Optional<byte[]> get(@NotNull UUID uuid) throws SQLException {
        String sql = """
                SELECT `data`
                FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, UUIDUtil.uuidToBytes(uuid));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                return Optional.of(resultSet.getBytes(1));
            }
        }
    }

    public boolean contains(@NotNull UUID uuid) throws SQLException {
        String sql = """
                SELECT 1
                FROM `%s`
                WHERE `id` = ?
                LIMIT 1
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, UUIDUtil.uuidToBytes(uuid));

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public void removeAll(Queue<UUID> queue, int limit, Consumer<UUID> c) throws SQLException{
        String sql = """
                DELETE FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);

            UUID p;
            while (limit-- > 0 && (p = queue.poll()) != null){
                statement.setBytes(1, UUIDUtil.uuidToBytes(p));
                c.accept(p);
                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
        }
    }

    public boolean remove(@NotNull UUID uuid) throws SQLException {
        String sql = """
                DELETE FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, UUIDUtil.uuidToBytes(uuid));

            return statement.executeUpdate() > 0;
        }
    }
}