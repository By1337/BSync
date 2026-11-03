package dev.by1337.sync.server.database.table;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.transfer.TransferEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Predicate;

/** Inbox and permanent deduplication receipts share one atomic INSERT. */
public final class ReliableMailboxRepository {
    private final HikariDataSource source;
    private final String table;

    public ReliableMailboxRepository(HikariDataSource source, String table) throws SQLException {
        this.source = source;
        this.table = quote(table);
        try (var connection = source.getConnection(); var sql = connection.createStatement()) {
            sql.execute("CREATE TABLE IF NOT EXISTS " + this.table + " ("
                    + "id BINARY(16) PRIMARY KEY, seq BIGINT AUTO_INCREMENT UNIQUE, owner BINARY(16) NOT NULL, "
                    + "payload LONGTEXT, fingerprint BINARY(32) NOT NULL, delivered BOOLEAN NOT NULL DEFAULT FALSE, "
                    + "attempts BIGINT NOT NULL DEFAULT 0, last_error TEXT, next_attempt BIGINT NOT NULL DEFAULT 0, "
                    + "INDEX " + quote("pending_" + Integer.toUnsignedString(table.hashCode(), 16)) + " (owner, delivered, seq))");
        }
    }

    public void migrateLegacy(String legacyTable) throws SQLException {
        // Creating the legacy table if missing also makes a first installation a no-op.
        new MailboxRepository(source, legacyTable);
        var copied = new ArrayList<Mail>();
        try (var connection = source.getConnection(); var sql = connection.createStatement();
            var rows = sql.executeQuery("SELECT uuid, id, mail FROM " + quote(legacyTable) + " ORDER BY id")) {
            while (rows.next()) {
                UUID owner = uuid(rows.getBytes(1));
                int oldId = rows.getInt(2);
                UUID id = UUID.nameUUIDFromBytes((legacyTable + ":" + owner + ":" + oldId).getBytes(StandardCharsets.UTF_8));
                copied.add(new Mail(id, owner, rows.getString(3)));
            }
        }
        for (Mail mail : copied) {
            if (!put(mail)) throw new SQLException("Conflicting legacy mail " + mail.id());
        }
        // Every copied row is now durable; retries of migration use the same IDs.
        try (var connection = source.getConnection(); var sql = connection.createStatement()) {
            connection.setAutoCommit(true);
            sql.executeUpdate("DELETE FROM " + quote(legacyTable));
        }
    }

    public boolean put(Mail mail) throws SQLException {
        byte[] hash = fingerprint(mail);
        try (var connection = source.getConnection(); var sql = connection.prepareStatement(
                "INSERT INTO " + table + " (id, owner, payload, fingerprint) VALUES (?, ?, ?, ?)")) {
            connection.setAutoCommit(true);
            sql.setBytes(1, bytes(mail.id()));
            sql.setBytes(2, bytes(mail.owner()));
            sql.setString(3, mail.payload());
            sql.setBytes(4, hash);
            try {
                sql.executeUpdate(); // autocommit finishes before confirmation
                return true;
            } catch (SQLException error) {
                if (error.getSQLState() == null || !error.getSQLState().startsWith("23")) throw error;
            }
        }
        try (var connection = source.getConnection(); var sql = connection.prepareStatement(
                "SELECT fingerprint FROM " + table + " WHERE id = ?")) {
            connection.setAutoCommit(true);
            sql.setBytes(1, bytes(mail.id()));
            try (var rows = sql.executeQuery()) {
                return rows.next() && MessageDigest.isEqual(hash, rows.getBytes(1));
            }
        }
    }

    /** The first unprocessed mail blocks later messages for the same player during backoff. */
    public Pending next(UUID owner) throws SQLException {
        try (var connection = source.getConnection(); var sql = connection.prepareStatement(
                "SELECT id, payload, attempts, next_attempt FROM " + table
                        + " WHERE owner = ? AND delivered = FALSE ORDER BY seq LIMIT 1")) {
            sql.setBytes(1, bytes(owner));
            try (var rows = sql.executeQuery()) {
                if (!rows.next()) return null;
                return new Pending(new Mail(uuid(rows.getBytes(1)), owner, rows.getString(2)), rows.getLong(3), rows.getLong(4));
            }
        }
    }

    public Page exportPage(long afterSequence) throws SQLException {
        return exportPage(afterSequence, key -> true);
    }

    public Page exportPage(long afterSequence, Predicate<UUID> include) throws SQLException {
        var entries = new ArrayList<Mail>();
        long cursor = afterSequence;
        int bytes = 0;
        int scanned = 0;
        boolean full = false;
        try (var connection = source.getConnection(); var statement = connection.prepareStatement(
                "SELECT seq, id, owner, payload FROM " + table
                        + " WHERE delivered = FALSE AND seq > ? ORDER BY seq LIMIT 256",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            statement.setFetchSize(connection.getMetaData().getDriverName().toLowerCase(Locale.ROOT).contains("mysql")
                    ? Integer.MIN_VALUE : 1);
            statement.setLong(1, afterSequence);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    scanned++;
                    UUID owner = uuid(rows.getBytes(3));
                    if (!include.test(owner)) {
                        cursor = rows.getLong(1);
                        continue;
                    }
                    String payload = rows.getString(4);
                    int length = payload.getBytes(StandardCharsets.UTF_8).length;
                    if (!entries.isEmpty() && bytes + length > TransferEntry.PAGE_BYTES) {
                        full = true;
                        break;
                    }
                    cursor = rows.getLong(1);
                    entries.add(new Mail(uuid(rows.getBytes(2)), owner, payload));
                    bytes += length;
                    if (bytes >= TransferEntry.PAGE_BYTES) {
                        full = true;
                        break;
                    }
                }
            }
        }
        return new Page(List.copyOf(entries), cursor, !full && scanned < 256);
    }

    public record Page(List<Mail> entries, long afterSequence, boolean done) {
    }

    public void acknowledge(UUID id) throws SQLException {
        try (var connection = source.getConnection(); var sql = connection.prepareStatement(
                "UPDATE " + table + " SET delivered = TRUE, payload = NULL, last_error = NULL WHERE id = ?")) {
            connection.setAutoCommit(true);
            sql.setBytes(1, bytes(id));
            sql.executeUpdate(); // keep fingerprint forever, including after restart
        }
    }

    public void failed(UUID id, String error, long retryAt) throws SQLException {
        try (var connection = source.getConnection(); var sql = connection.prepareStatement(
                "UPDATE " + table + " SET attempts = attempts + 1, last_error = ?, next_attempt = ?"
                        + " WHERE id = ? AND delivered = FALSE")) {
            connection.setAutoCommit(true);
            sql.setString(1, error);
            sql.setLong(2, retryAt);
            sql.setBytes(3, bytes(id));
            sql.executeUpdate();
        }
    }

    private static byte[] fingerprint(Mail mail) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes(mail.owner()));
            return digest.digest(mail.payload().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static byte[] bytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }

    private static UUID uuid(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    public record Pending(Mail mail, long attempts, long retryAt) {
    }
}
