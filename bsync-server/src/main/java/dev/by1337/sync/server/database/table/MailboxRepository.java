package dev.by1337.sync.server.database.table;

import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class MailboxRepository {

    private final HikariDataSource dataSource;
    private final String table;

    public MailboxRepository(HikariDataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.table = table;
        try {
            createTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable() throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS %s (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                
                    owner BINARY(16) NOT NULL,
                
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                
                    payload MEDIUMBLOB NOT NULL,
                
                    PRIMARY KEY (id),
                
                    INDEX idx_owner_id (owner, id)
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
                """.formatted(table);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
        }
    }

    public long insert(@NotNull UUID owner, byte @NotNull [] payload) throws SQLException {
        String sql = """
                    INSERT INTO `%s` (`owner`, `payload`)
                    VALUES (?, ?)
                """.formatted(table);

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            ps.setBytes(1, uuidToBytes(owner));
            ps.setBytes(2, payload);

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (!rs.next()) {
                    throw new SQLException("No generated key returned");
                }
                return rs.getLong(1);
            }
        }
    }

    public Optional<Message> getById(long id) throws SQLException {
        String sql = """
                    SELECT id, owner, payload
                    FROM `%s`
                    WHERE id = ?
                """.formatted(table);

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setLong(1, id);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();

                return Optional.of(new Message(
                        rs.getLong("id"),
                        bytesToUuid(rs.getBytes("owner")),
                        rs.getBytes("payload")
                ));
            }
        }
    }

    public List<Message> getAll(UUID owner, long afterId, int limit) throws SQLException {
        String sql = """
                    SELECT id, owner, payload
                    FROM `%s`
                    WHERE owner = ?
                      AND id > ?
                    ORDER BY id ASC
                    LIMIT ?
                """.formatted(table);

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setBytes(1, uuidToBytes(owner));
            ps.setLong(2, afterId);
            ps.setInt(3, limit);

            try (ResultSet rs = ps.executeQuery()) {
                List<Message> list = new ArrayList<>();

                while (rs.next()) {
                    list.add(new Message(
                            rs.getLong("id"),
                            owner,
                            rs.getBytes("payload")
                    ));
                }

                return list;
            }
        }
    }

    public boolean delete(long id) throws SQLException {
        String sql = """
                    DELETE FROM `%s`
                    WHERE id = ?
                """.formatted(table);

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setLong(1, id);

            return ps.executeUpdate() > 0;
        }
    }

    public boolean delete(UUID owner, long id) throws SQLException {
        String sql = """
                    DELETE FROM `%s`
                    WHERE owner = ?
                      AND id = ?
                """.formatted(table);

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setBytes(1, uuidToBytes(owner));
            ps.setLong(2, id);

            return ps.executeUpdate() > 0;
        }
    }

    public record Message(long id, UUID owner, byte[] payload) {}

    private static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];

        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (msb >>> (56 - i * 8));
            bytes[i + 8] = (byte) (lsb >>> (56 - i * 8));
        }

        return bytes;
    }

    private static UUID bytesToUuid(byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("UUID must be 16 bytes");
        }

        long msb = 0;
        long lsb = 0;

        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (bytes[i] & 0xFFL);
        }

        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (bytes[i] & 0xFFL);
        }

        return new UUID(msb, lsb);
    }
}