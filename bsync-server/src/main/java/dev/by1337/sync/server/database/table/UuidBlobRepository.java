package dev.by1337.sync.server.database.table;

import com.zaxxer.hikari.HikariDataSource;
import org.jspecify.annotations.NonNull;

import java.sql.*;
import java.util.Optional;
import java.util.UUID;

public final class UuidBlobRepository implements K2VTable<UUID, byte[]> {
    private final HikariDataSource dataSource;
    private final String tableName;

    public UuidBlobRepository(HikariDataSource dataSource, String tableName) {
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

    public void put(@NonNull UUID uuid, byte @NonNull [] data) throws SQLException {
        String sql = """
                INSERT INTO `%s` (`id`, `data`)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE
                    `data` = VALUES(`data`)
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(uuid));
            statement.setBytes(2, data);

            statement.executeUpdate();
        }
    }

    public @NonNull Optional<byte[]> get(@NonNull UUID uuid) throws SQLException {
        String sql = """
                SELECT `data`
                FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(uuid));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                return Optional.of(resultSet.getBytes(1));
            }
        }
    }

    public boolean contains(@NonNull UUID uuid) throws SQLException {
        String sql = """
                SELECT 1
                FROM `%s`
                WHERE `id` = ?
                LIMIT 1
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(uuid));

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    @Override
    public void close() throws SQLException {
    }

    public boolean delete(@NonNull UUID uuid) throws SQLException {
        String sql = """
                DELETE FROM `%s`
                WHERE `id` = ?
                """.formatted(tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(uuid));

            return statement.executeUpdate() > 0;
        }
    }

    private static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];

        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();

        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (most >>> (56 - (i * 8)));
        }

        for (int i = 0; i < 8; i++) {
            bytes[i + 8] = (byte) (least >>> (56 - (i * 8)));
        }

        return bytes;
    }

    public static UUID bytesToUuid(byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("UUID byte array must be 16 bytes");
        }

        long most = 0;
        long least = 0;

        for (int i = 0; i < 8; i++) {
            most = (most << 8) | (bytes[i] & 0xFFL);
        }

        for (int i = 8; i < 16; i++) {
            least = (least << 8) | (bytes[i] & 0xFFL);
        }

        return new UUID(most, least);
    }
}