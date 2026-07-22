package dev.by1337.sync.server.database.table;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;

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
                    uuid BINARY(16) NOT NULL,
                    id INT UNSIGNED NOT NULL,
                    mail TEXT NOT NULL,
                
                    PRIMARY KEY (uuid, id)
                );""".formatted(table);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
        }
    }

    public int getMaxId() throws SQLException {
        String sql = "SELECT MAX(id) FROM %s;".formatted(table);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {

            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    public List<Mail> loadAll(UUID owner) throws SQLException {
        String sql = """
            SELECT id, mail
            FROM %s
            WHERE uuid = ?
            ORDER BY id
            """.formatted(table);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(owner));

            try (ResultSet result = statement.executeQuery()) {
                List<Mail> mails = new ArrayList<>();

                while (result.next()) {
                    mails.add(new Mail(
                            result.getInt("id"),
                            owner,
                            result.getString("mail")
                    ));
                }

                return mails;
            }
        }
    }

    public void put(int id, UUID owner, String payload) throws SQLException {
        put(new Mail(id, owner, payload));
    }

    public void put(Mail mail) throws SQLException {
        String sql = """
                INSERT INTO `%s` (uuid, id, mail)
                VALUES (?, ?, ?)
                """.formatted(table);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(mail.owner));
            statement.setInt(2, mail.id);
            statement.setString(3, mail.payload);

            statement.executeUpdate();
        }
    }

    public void putAll(Queue<Mail> queue, int limit, Consumer<Mail> c) throws SQLException {
        if (queue.isEmpty()) return;
        String sql = """
                INSERT INTO `%s` (uuid, id, mail)
                VALUES (?, ?, ?)
                """.formatted(table);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);
            Mail mail;
            while (limit-- > 0 && (mail = queue.poll()) != null) {
                statement.setBytes(1, uuidToBytes(mail.owner));
                statement.setInt(2, mail.id);
                statement.setString(3, mail.payload);
                c.accept(mail);
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }
    }

    public void remove(Mail mail) throws SQLException {
        String sql = """
                DELETE FROM %s
                WHERE uuid = ? AND id = ?;""".formatted(table);


        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setBytes(1, uuidToBytes(mail.owner));
            statement.setInt(2, mail.id);

            statement.executeUpdate();
        }
    }
    public void removeAll(Queue<Mail> queue, int limit, Consumer<Mail> c) throws SQLException {
        String sql = """
                DELETE FROM %s
                WHERE uuid = ? AND id = ?;""".formatted(table);


        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);
            Mail mail;
            while (limit-- > 0 && (mail = queue.poll()) != null) {
                statement.setBytes(1, uuidToBytes(mail.owner));
                statement.setInt(2, mail.id);
                c.accept(mail);
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }
    }


    public record Mail(int id, UUID owner, String payload) {}

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