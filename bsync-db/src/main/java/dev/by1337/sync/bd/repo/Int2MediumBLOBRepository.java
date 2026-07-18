package dev.by1337.sync.bd.repo;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.bd.table.K2VPair;
import dev.by1337.sync.bd.table.K2VTable;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

public final class Int2MediumBLOBRepository implements K2VTable<Integer, byte[]> {
    private final HikariDataSource dataSource;
    private final String tableName;

    public Int2MediumBLOBRepository(HikariDataSource dataSource, String tableName) {
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
                    `id` INT NOT NULL,
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

    public int getMaxId() throws SQLException {
        String sql = "SELECT MAX(id) FROM %s;".formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {

            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    public void putAll(Queue<K2VPair<Integer, byte[]>> queue, int limit, Consumer<K2VPair<Integer, byte[]>> c) throws SQLException {
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

            K2VPair<Integer, byte @NotNull []> p;
            while (limit-- > 0 && (p = queue.poll()) != null) {
                statement.setInt(1, p.key);
                statement.setBytes(2, p.value);
                c.accept(p);
                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
        }
    }

    public void put(@NotNull Integer id, byte @NotNull [] data) throws SQLException {
        String sql = """
                INSERT INTO `%s` (`id`, `data`)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE
                    `data` = VALUES(`data`)
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setInt(1, id);
            statement.setBytes(2, data);

            statement.executeUpdate();
        }
    }

    public @NotNull Optional<byte[]> get(@NotNull Integer key) throws SQLException {
        String sql = """
                SELECT `data`
                FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setInt(1, key);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                return Optional.of(resultSet.getBytes(1));
            }
        }
    }

    public boolean contains(@NotNull Integer id) throws SQLException {
        String sql = """
                SELECT 1
                FROM `%s`
                WHERE `id` = ?
                LIMIT 1
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setInt(1, id);

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public void removeAll(Queue<Integer> queue, int limit, Consumer<Integer> c) throws SQLException {
        String sql = """
                DELETE FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);

            Integer p;
            while (limit-- > 0 && (p = queue.poll()) != null) {
                statement.setInt(1, p);
                c.accept(p);
                statement.addBatch();
            }

            statement.executeBatch();
            connection.commit();
        }
    }

    public boolean remove(Integer id) throws SQLException {
        String sql = """
                DELETE FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setInt(1, id);

            return statement.executeUpdate() > 0;
        }
    }
}