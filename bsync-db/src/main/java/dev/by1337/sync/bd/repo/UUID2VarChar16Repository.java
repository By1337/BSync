package dev.by1337.sync.bd.repo;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.bd.table.K2VTable;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Optional;
import java.util.UUID;

public final class UUID2VarChar16Repository implements K2VTable<UUID, String> {
    private final HikariDataSource dataSource;
    private final String tableName;

    public UUID2VarChar16Repository(HikariDataSource dataSource, String tableName) {
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
                    `data` VARCHAR(16) NOT NULL,
                
                    PRIMARY KEY (`id`)
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
        }
    }

    public void put(@NotNull UUID uuid, @NotNull String data) throws SQLException {
        String sql = """
                INSERT INTO `%s` (`id`, `data`)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE
                    `data` = VALUES(`data`)
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, UUIDUtil.uuidToBytes(uuid));
            statement.setString(2, data);

            statement.executeUpdate();
        }
    }

    public @NotNull Optional<String> get(@NotNull UUID uuid) throws SQLException {
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

                return Optional.of(resultSet.getString(1));
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

    public boolean delete(@NotNull UUID uuid) throws SQLException {
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