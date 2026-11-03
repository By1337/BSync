package dev.by1337.sync.k2v.storage.sql;

import dev.by1337.sync.k2v.mail.Mail;
import dev.by1337.sync.k2v.transfer.ExportPage;
import dev.by1337.sync.k2v.transfer.TransferEntry;
import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Session tokens fence mutations; network SQL also requires an unexpired database-clock lease. */
final class SqlLeaseRepository implements AutoCloseable {
    static final String PLAYERS = "bsync_player_data";
    static final String MAILS = "bsync_player_mailbox";
    private static final ConcurrentMap<Path, SqlLeaseRepository> OPEN_SQLITE = new ConcurrentHashMap<>();
    private final DataSource data, heartbeat;
    private final SqlStorageConfig config;
    private FileChannel lockChannel;
    private FileLock fileLock;
    private Path registeredLock;
    private boolean closed, sqliteInitialized;

    SqlLeaseRepository(DataSource data, DataSource heartbeat, SqlStorageConfig config) {
        this.data = data;
        this.heartbeat = heartbeat;
        this.config = config;
    }

    synchronized void initialize(boolean mailbox) throws SQLException {
        if (closed) throw new SQLException("SQL repository is closed");
        if (config.isSqlite()) {
            if (sqliteInitialized) return;
            lockSqliteFile();
        }
        try (Connection connection = data.getConnection(); Statement statement = connection.createStatement()) {
            if (!config.isSqlite()) statement.setQueryTimeout(timeout());
            if (config.isSqlite()) {
                initializeSqlite(statement, mailbox);
                // The exclusive process lock proves the previous local owner is gone.
                // Keep snapshots/mail receipts, discard only abandoned session tokens.
                try (var reset = prepare(connection, "UPDATE " + PLAYERS
                        + " SET owner_token=NULL,lease_until=NULL WHERE repository=? AND owner_token IS NOT NULL")) {
                    reset.setString(1, config.repository);
                    reset.executeUpdate();
                }
                sqliteInitialized = true;
                return;
            }
            statement.execute("CREATE TABLE IF NOT EXISTS " + PLAYERS + " ("
                    + "repository VARCHAR(96) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, "
                    + "player_id BINARY(16) NOT NULL, data MEDIUMBLOB, owner_token BINARY(16), "
                    + "owner_epoch BIGINT NOT NULL DEFAULT 0, lease_until DATETIME(3), "
                    + "PRIMARY KEY(repository, player_id), INDEX owned_players(repository,owner_token)) ENGINE=InnoDB");
            if (mailbox) statement.execute("CREATE TABLE IF NOT EXISTS " + MAILS + " ("
                    + "repository VARCHAR(96) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, "
                    + "id BINARY(16) NOT NULL, seq BIGINT NOT NULL AUTO_INCREMENT UNIQUE, player_id BINARY(16) NOT NULL, "
                    + "payload LONGTEXT CHARACTER SET utf8mb4, fingerprint BINARY(32) NOT NULL, "
                    + "delivered BOOLEAN NOT NULL DEFAULT FALSE, attempts BIGINT NOT NULL DEFAULT 0, "
                    + "next_attempt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), last_error VARCHAR(1024), "
                    + "PRIMARY KEY(repository,id), INDEX pending_player(repository,player_id,delivered,seq), "
                    + "INDEX pending_retry(repository,delivered,next_attempt,seq)) ENGINE=InnoDB");
        } catch (SQLException | RuntimeException e) {
            if (config.isSqlite()) {
                try {
                    releaseFileLock();
                } catch (SQLException cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    private void lockSqliteFile() throws SQLException {
        try {
            Path database = Path.of(config.database);
            Files.createDirectories(database.getParent());
            Path canonical = Files.exists(database) ? database.toRealPath()
                    : database.getParent().toRealPath().resolve(database.getFileName());
            String namespace = UUID.nameUUIDFromBytes(config.repository.getBytes(StandardCharsets.UTF_8)).toString();
            Path lockPath = canonical.resolveSibling(canonical.getFileName() + "." + namespace + ".lock");
            // Avoid opening/closing a second channel for a lock already held in this
            // JVM: some operating systems release process locks when any channel closes.
            if (OPEN_SQLITE.putIfAbsent(lockPath, this) != null) throw new IOException("SQLite repository is already open");
            registeredLock = lockPath;
            lockChannel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            fileLock = lockChannel.tryLock();
            if (fileLock == null) throw new IOException("SQLite repository is already open");
        } catch (IOException | OverlappingFileLockException e) {
            var failure = new SQLException("SQLite repository is already open or its lock file is unavailable", e);
            try {
                releaseFileLock();
            } catch (SQLException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void releaseFileLock() throws SQLException {
        try {
            if (lockChannel != null) lockChannel.close();
        } catch (IOException e) {
            throw new SQLException("Cannot release SQLite process lock", e);
        } finally {
            lockChannel = null;
            fileLock = null;
            if (registeredLock != null) OPEN_SQLITE.remove(registeredLock, this);
            registeredLock = null;
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        closed = true;
        releaseFileLock();
    }

    private String activeOwner(String prefix) {
        return config.isSqlite() ? "1=1" : prefix + "lease_until>" + currentTime();
    }

    private void initializeSqlite(Statement statement, boolean mailbox) throws SQLException {
        statement.execute("CREATE TABLE IF NOT EXISTS " + PLAYERS + " ("
                + "repository TEXT NOT NULL, player_id BLOB NOT NULL, data BLOB, owner_token BLOB, "
                + "owner_epoch INTEGER NOT NULL DEFAULT 0, lease_until INTEGER, PRIMARY KEY(repository,player_id))");
        statement.execute("CREATE INDEX IF NOT EXISTS owned_players ON " + PLAYERS + " (repository,owner_token)");
        if (!mailbox) return;
        statement.execute("CREATE TABLE IF NOT EXISTS " + MAILS + " ("
                + "seq INTEGER PRIMARY KEY AUTOINCREMENT, repository TEXT NOT NULL, id BLOB NOT NULL, player_id BLOB NOT NULL, "
                + "payload TEXT, fingerprint BLOB NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, "
                + "next_attempt INTEGER NOT NULL, last_error TEXT, UNIQUE(repository,id))");
        statement.execute("CREATE INDEX IF NOT EXISTS pending_player ON " + MAILS + " (repository,player_id,delivered,seq)");
        statement.execute("CREATE INDEX IF NOT EXISTS pending_retry ON " + MAILS + " (repository,delivered,next_attempt,seq)");
    }

    private String currentTime() {
        return config.isSqlite() ? "CAST(unixepoch('subsec') * 1000000 AS INTEGER)" : "UTC_TIMESTAMP(3)";
    }

    private String expiresAt() {
        return config.isSqlite() ? "(" + currentTime() + "+?)" : "TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(3))";
    }

    Claim claim(UUID player, UUID token) throws SQLException {
        try (Connection connection = data.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement insert = prepare(connection, "INSERT INTO " + PLAYERS
                        + " (repository,player_id) VALUES (?,?) " + (config.isSqlite()
                                ? "ON CONFLICT(repository,player_id) DO NOTHING" : "ON DUPLICATE KEY UPDATE player_id=player_id"))) {
                    insert.setString(1, config.repository);
                    insert.setBytes(2, bytes(player));
                    insert.executeUpdate();
                }
                try (PreparedStatement acquire = prepare(connection, "UPDATE " + PLAYERS
                        + " SET owner_token=?, owner_epoch=owner_epoch+1, lease_until=" + (config.isSqlite() ? "NULL" : expiresAt())
                        + " WHERE repository=? AND player_id=? AND (owner_token IS NULL"
                        + (config.isSqlite() ? "" : " OR lease_until<=" + currentTime()) + ")")) {
                    acquire.setBytes(1, bytes(token));
                    int index = 2;
                    if (!config.isSqlite()) acquire.setLong(index++, config.leaseMillis * 1000);
                    acquire.setString(index++, config.repository);
                    acquire.setBytes(index, bytes(player));
                    acquire.executeUpdate();
                }
                // Also recognizes a committed acquisition whose response was lost. It never
                // assumes the full TTL remains when resuming that same token.
                Claim result = null;
                try (PreparedStatement read = prepare(connection, "SELECT owner_epoch,data,"
                        + (config.isSqlite() ? Long.toString(config.leaseMillis * 1000) : "TIMESTAMPDIFF(MICROSECOND,UTC_TIMESTAMP(3),lease_until)")
                        + " FROM " + PLAYERS
                        + " WHERE repository=? AND player_id=? AND owner_token=? AND " + activeOwner(""))) {
                    read.setString(1, config.repository);
                    read.setBytes(2, bytes(player));
                    read.setBytes(3, bytes(token));
                    try (ResultSet rows = read.executeQuery()) {
                        if (rows.next()) result = new Claim(rows.getLong(1), rows.getBytes(2), rows.getLong(3) / 1000);
                    }
                }
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                rollback(connection, e);
                throw e;
            }
        }
    }

    boolean renew(UUID player, UUID token, long epoch) throws SQLException {
        if (config.isSqlite()) throw new SQLException("Local SQLite ownership does not require lease renewal");
        try (Connection connection = heartbeat.getConnection(); PreparedStatement update = prepare(connection,
                "UPDATE " + PLAYERS + " SET lease_until=" + expiresAt()
                        + " WHERE repository=? AND player_id=? AND owner_token=? AND owner_epoch=? AND " + activeOwner(""))) {
            connection.setAutoCommit(true);
            update.setLong(1, config.leaseMillis * 1000);
            bindLease(update, 2, player, token, epoch);
            return update.executeUpdate() == 1;
        }
    }

    boolean save(UUID player, UUID token, long epoch, byte[] snapshot, boolean release) throws SQLException {
        String assignments = "data=COALESCE(?,data)" + (release ? ",owner_token=NULL,lease_until=NULL" : "");
        try (Connection connection = data.getConnection(); PreparedStatement update = prepare(connection,
                "UPDATE " + PLAYERS + " SET " + assignments
                        + " WHERE repository=? AND player_id=? AND owner_token=? AND owner_epoch=? AND " + activeOwner(""))) {
            connection.setAutoCommit(true);
            update.setBytes(1, snapshot);
            bindLease(update, 2, player, token, epoch);
            return update.executeUpdate() == 1;
        }
    }

    byte[] snapshot(UUID player) throws SQLException {
        try (Connection connection = data.getConnection(); PreparedStatement read = prepare(connection,
                "SELECT data FROM " + PLAYERS + " WHERE repository=? AND player_id=?")) {
            read.setString(1, config.repository);
            read.setBytes(2, bytes(player));
            try (ResultSet rows = read.executeQuery()) {
                return rows.next() ? rows.getBytes(1) : null;
            }
        }
    }

    boolean admit(Mail mail) throws SQLException {
        byte[] fingerprint = fingerprint(mail);
        try (Connection connection = data.getConnection(); PreparedStatement insert = prepare(connection,
                "INSERT INTO " + MAILS + " (repository,id,player_id,payload,fingerprint,next_attempt) VALUES (?,?,?,?,?," + currentTime() + ")"
                        + (config.isSqlite() ? " ON CONFLICT(repository,id) DO NOTHING" : ""))) {
            connection.setAutoCommit(true);
            insert.setString(1, config.repository);
            insert.setBytes(2, bytes(mail.id()));
            insert.setBytes(3, bytes(mail.owner()));
            insert.setString(4, mail.payload());
            insert.setBytes(5, fingerprint);
            try {
                if (insert.executeUpdate() == 1) return true;
            } catch (SQLException e) {
                if (e.getSQLState() == null || !e.getSQLState().startsWith("23")) throw e;
            }
        }
        try (Connection connection = data.getConnection(); PreparedStatement read = prepare(connection,
                "SELECT fingerprint FROM " + MAILS + " WHERE repository=? AND id=?")) {
            read.setString(1, config.repository);
            read.setBytes(2, bytes(mail.id()));
            try (ResultSet rows = read.executeQuery()) {
                return rows.next() && MessageDigest.isEqual(fingerprint, rows.getBytes(1));
            }
        }
    }

    List<PendingMail> pending(List<UUID> tokens) throws SQLException {
        if (tokens.isEmpty()) return List.of();
        String placeholders = String.join(",", Collections.nCopies(tokens.size(), "?"));
        String query = "SELECT m.id,m.player_id,m.payload,m.attempts,p.owner_token,p.owner_epoch FROM " + MAILS + " m JOIN " + PLAYERS
                + " p ON p.repository=m.repository AND p.player_id=m.player_id WHERE m.repository=? AND m.delivered=FALSE"
                + " AND p.owner_token IN (" + placeholders + ") AND " + activeOwner("p.") + " AND m.next_attempt<=" + currentTime()
                + " AND NOT EXISTS (SELECT 1 FROM " + MAILS + " older WHERE older.repository=m.repository"
                + " AND older.player_id=m.player_id AND older.delivered=FALSE AND older.seq<m.seq) ORDER BY m.seq LIMIT 64";
        try (Connection connection = data.getConnection(); PreparedStatement read = prepare(connection, query)) {
            read.setString(1, config.repository);
            for (int i = 0; i < tokens.size(); i++) read.setBytes(i + 2, bytes(tokens.get(i)));
            var result = new ArrayList<PendingMail>();
            try (ResultSet rows = read.executeQuery()) {
                while (rows.next()) result.add(new PendingMail(new Mail(uuid(rows.getBytes(1)), uuid(rows.getBytes(2)), rows.getString(3)),
                        rows.getLong(4), uuid(rows.getBytes(5)), rows.getLong(6)));
            }
            return result;
        }
    }

    boolean acknowledge(PendingMail mail) throws SQLException {
        return updateMail(mail, true, "", 0);
    }

    void failed(PendingMail mail, String error, long retryMillis) throws SQLException {
        updateMail(mail, false, error.length() > 1000 ? error.substring(0, 1000) : error, retryMillis);
    }

    private boolean updateMail(PendingMail pending, boolean accepted, String error, long retryMillis) throws SQLException {
        String prefix = config.isSqlite() ? "" : "m.";
        String assignments = accepted ? prefix + "delivered=TRUE," + prefix + "payload=NULL," + prefix + "last_error=NULL"
                : prefix + "attempts=m.attempts+1," + prefix + "last_error=?," + prefix + "next_attempt=" + expiresAt();
        String query = config.isSqlite()
                ? "UPDATE " + MAILS + " AS m SET " + assignments
                        + " WHERE m.repository=? AND m.id=? AND m.delivered=FALSE AND EXISTS (SELECT 1 FROM " + PLAYERS
                        + " p WHERE p.repository=m.repository AND p.player_id=m.player_id AND p.owner_token=? AND p.owner_epoch=?"
                        + " AND " + activeOwner("p.") + ")"
                : "UPDATE " + MAILS + " m JOIN " + PLAYERS + " p ON p.repository=m.repository AND p.player_id=m.player_id SET " + assignments
                        + " WHERE m.repository=? AND m.id=? AND m.delivered=FALSE AND p.owner_token=? AND p.owner_epoch=?"
                        + " AND p.lease_until>" + currentTime();
        try (Connection connection = data.getConnection(); PreparedStatement update = prepare(connection, query)) {
            connection.setAutoCommit(true);
            int index = 1;
            if (!accepted) {
                update.setString(index++, error);
                update.setLong(index++, retryMillis * 1000);
            }
            update.setString(index++, config.repository);
            update.setBytes(index++, bytes(pending.mail.id()));
            update.setBytes(index++, bytes(pending.token));
            update.setLong(index, pending.epoch);
            return update.executeUpdate() == 1;
        }
    }

    ExportPage exportData(UUID after) throws SQLException {
        var entries = new ArrayList<TransferEntry>();
        UUID cursor = after;
        int size = 0;
        boolean full = false;
        try (Connection connection = data.getConnection(); PreparedStatement read = streaming(connection,
                "SELECT player_id,data FROM " + PLAYERS + " WHERE repository=? AND data IS NOT NULL"
                        + (after == null ? "" : " AND player_id>?") + " ORDER BY player_id LIMIT 256")) {
            read.setString(1, config.repository);
            if (after != null) read.setBytes(2, bytes(after));
            try (ResultSet rows = read.executeQuery()) {
                while (rows.next()) {
                    byte[] payload = rows.getBytes(2);
                    if (!entries.isEmpty() && size + payload.length > TransferEntry.PAGE_BYTES) {
                        full = true;
                        break;
                    }
                    cursor = uuid(rows.getBytes(1));
                    entries.add(new TransferEntry(TransferEntry.Kind.DATA, cursor, payload, null));
                    size += payload.length;
                    if (size >= TransferEntry.PAGE_BYTES) {
                        full = true;
                        break;
                    }
                }
            }
        }
        return new ExportPage(List.copyOf(entries), cursor, 0, !full && entries.size() < 256, "");
    }

    ExportPage exportMails(long after) throws SQLException {
        var entries = new ArrayList<TransferEntry>();
        long cursor = after;
        int size = 0;
        boolean full = false;
        try (Connection connection = data.getConnection(); PreparedStatement read = streaming(connection,
                "SELECT seq,id,player_id,payload FROM " + MAILS + " WHERE repository=? AND delivered=FALSE AND seq>? ORDER BY seq LIMIT 256")) {
            read.setString(1, config.repository);
            read.setLong(2, after);
            try (ResultSet rows = read.executeQuery()) {
                while (rows.next()) {
                    byte[] payload = rows.getString(4).getBytes(StandardCharsets.UTF_8);
                    if (!entries.isEmpty() && size + payload.length > TransferEntry.PAGE_BYTES) {
                        full = true;
                        break;
                    }
                    cursor = rows.getLong(1);
                    entries.add(new TransferEntry(TransferEntry.Kind.MAIL, uuid(rows.getBytes(3)), payload, uuid(rows.getBytes(2))));
                    size += payload.length;
                    if (size >= TransferEntry.PAGE_BYTES) {
                        full = true;
                        break;
                    }
                }
            }
        }
        return new ExportPage(List.copyOf(entries), null, cursor, !full && entries.size() < 256, "");
    }

    private PreparedStatement prepare(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (!config.isSqlite()) statement.setQueryTimeout(timeout());
        return statement;
    }

    private PreparedStatement streaming(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (!config.isSqlite()) statement.setQueryTimeout(Math.max(5, timeout()));
        statement.setFetchSize(config.scheme.equals("mysql") ? Integer.MIN_VALUE : 1);
        return statement;
    }

    private int timeout() {
        return (int) Math.max(1, (config.timeoutMillis + 999) / 1000);
    }

    private void bindLease(PreparedStatement sql, int first, UUID player, UUID token, long epoch) throws SQLException {
        sql.setString(first, config.repository);
        sql.setBytes(first + 1, bytes(player));
        sql.setBytes(first + 2, bytes(token));
        sql.setLong(first + 3, epoch);
    }

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            original.addSuppressed(e);
        }
    }

    private static byte[] fingerprint(Mail mail) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes(mail.owner()));
            return digest.digest(mail.payload().getBytes(StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] bytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }

    private static UUID uuid(byte[] bytes) {
        ByteBuffer value = ByteBuffer.wrap(bytes);
        return new UUID(value.getLong(), value.getLong());
    }

    record Claim(long epoch, byte[] data, long remainingMillis) {
    }

    record PendingMail(Mail mail, long attempts, UUID token, long epoch) {
    }
}
