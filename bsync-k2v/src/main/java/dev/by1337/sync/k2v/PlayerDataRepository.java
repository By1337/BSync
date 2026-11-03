package dev.by1337.sync.k2v;

import dev.by1337.sync.bukkit.BSync;
import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.k2v.storage.BSyncStorage;
import dev.by1337.sync.k2v.storage.FilePlayerDataStorage;
import dev.by1337.sync.k2v.storage.sql.JdbcPlayerDataStorage;
import dev.by1337.sync.k2v.storage.sql.SqlStorageConfig;
import dev.by1337.sync.k2v.transfer.TransferStats;
import org.bukkit.plugin.Plugin;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;

/**
 * Supported connection addresses include:
 *
 * <pre>{@code
 * local://players
 * group://player_data/players
 * server://main/players
 * mysql://localhost:3306/game/players?user=plugin&password=secret
 * mariadb://localhost:3306/game/players?user=plugin&password=secret
 * sqlite://players.db?repository=players
 * sqlite:///srv/minecraft/players.db?repository=players&poll=5s
 * mariadb://localhost:3306/game/players?user=plugin&password=secret&lease=30s&renew=27s&pool=4&poll=5s&timeout=1s&acquireWait=5s
 * }</pre>
 *
 * MySQL/MariaDB addresses connect directly to the database; the final path segment
 * is the repository namespace. Percent-encode special characters in credentials.
 * SQLite is local to one server: the URI specifies a file and an optional
 * repository namespace (default: players), with no periodic lease renewal.
 * Relative SQLite paths and its auxiliary files are stored in the plugin's data folder.
 * Local storage uses an existing bsync/plugin-name/storage-name directory when
 * present; otherwise it is created under the plugin's data folder.
 */
public interface PlayerDataRepository<T> {
    // local://<storage-name>
    // group://group/<storage-name>
    // server://id/<storage-name>
    static <T> PlayerDataRepository<T> create(String repo, Plugin plugin, DataManager<T> dataManager) {
        return create(repo, plugin, dataManager, Type.ALL);
    }

    static PlayerDataRepository<UUID> createMailBox(String repo, Plugin plugin, DataManager.MailBox dataManager) {
        return create(repo, plugin, dataManager, Type.ONLY_MAILBOX);
    }

    static <T> PlayerDataRepository<T> createOnlyData(String repo, Plugin plugin, DataManager.OnlyData<T> dataManager) {
        return create(repo, plugin, dataManager, Type.ONLY_BLOBS);
    }

    static <T> PlayerDataRepository<T> create(String repo, Plugin plugin, DataManager<T> dataManager, Type type) {
        if (repo.regionMatches(true, 0, "mysql://", 0, 8) || repo.regionMatches(true, 0, "mariadb://", 0, 10)
                || repo.regionMatches(true, 0, "sqlite://", 0, 9)) {
            var config = SqlStorageConfig.parse(repo, plugin.getDataFolder().toPath());
            String identity = UUID.nameUUIDFromBytes(config.journalIdentity().getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
            Path directory = config.isSqlite() ? plugin.getDataFolder().toPath().resolve("sql").resolve(identity)
                    : Path.of("bsync", plugin.getName(), "sql", identity);
            var storage = new JdbcPlayerDataStorage(config, directory,
                    type != Type.ONLY_MAILBOX, type != Type.ONLY_BLOBS);
            try {
                return new PlayerDataRepositoryImpl<>(storage, plugin, dataManager);
            } catch (RuntimeException | Error e) {
                storage.close();
                throw e;
            }
        } else if (repo.startsWith("local://")) {
            String storageName = repo.substring("local://".length());
            File legacyFolder = new File("./bsync/" + plugin.getName() + "/" + storageName);
            File folder = legacyFolder.exists() ? legacyFolder : new File(plugin.getDataFolder(), storageName);
            return new PlayerDataRepositoryImpl<>(
                    new FilePlayerDataStorage(folder),
                    plugin,
                    dataManager
            );
        } else if (repo.startsWith("group://")) {
            String[] args = repo.substring("group://".length()).split("/", 2);
            if (args.length != 2)
                throw new RuntimeException("Bad storage address! \"" + repo + "\" use \"group://group/<storage-name>\"");
            var bsyncType = switch (type) {
                case ALL -> Locks.Type.ALL;
                case ONLY_BLOBS -> Locks.Type.ONLY_BLOBS;
                case ONLY_MAILBOX -> Locks.Type.ONLY_MAILBOX;
            };
            var list = BSync.getGroup(args[0]);
            BSyncStorage bss = new BSyncStorage();
            ChannelMaker.ChannelData<Locks> locks = ChannelMaker.createGroupLocks(list, args[1], bss.asBSyncLockManager(), bsyncType);
            bss.setLocks(locks.get(), locks::close);
            //try to wait to connect
            int x = 50;
            while (x-- > 0 && !locks.get().isReady()) {
                LockSupport.parkNanos(2_000_000);
            }
            return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
        } else if (repo.startsWith("server://")) {
            String[] args = repo.substring("server://".length()).split("/", 2);
            if (args.length != 2)
                throw new RuntimeException("Bad storage address! \"" + repo + "\" use \"server://id/<storage-name>\"");
            var bsyncType = switch (type) {
                case ALL -> Locks.Type.ALL;
                case ONLY_BLOBS -> Locks.Type.ONLY_BLOBS;
                case ONLY_MAILBOX -> Locks.Type.ONLY_MAILBOX;
            };
            var conn = BSync.getConnection(args[0]);
            BSyncStorage bss = new BSyncStorage();
            ChannelMaker.ChannelData<Locks> locks = ChannelMaker.createLocks(conn, args[1], bss.asBSyncLockManager(), bsyncType);
            bss.setLocks(locks.get(), locks::close);
            int x = 50;
            while (x-- > 0 && !locks.get().isReady()) {
                LockSupport.parkNanos(2_000_000);
            }
            return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
        } else {
            throw new IllegalArgumentException("Unknown storage address; use local://, group://, server://, mysql://, mariadb:// or sqlite://");
        }
    }

    void close();

    @Nullable
    T getUser(UUID key);

    /**
     * Durably admits a mail to the local outbox before returning. Delivery is retried
     * internally across disconnects and restarts, with a stable message ID.
     * A successful return confirms admission, not execution of the recipient's handler.
     * Storage/admission errors are thrown synchronously; accepted mails are never expired.
     */
    void pushMail(UUID key, String mail);

    void pushSnapshot(UUID key, T user);

    CompletableFuture<@Nullable T> loadSnapshot(UUID key);

    /** Export persisted snapshots and pending mails, streaming directly from storage/SQL. */
    CompletableFuture<TransferStats> exportTo(Path file);

    default CompletableFuture<TransferStats> importFrom(Path file) {
        return importFrom(file, 256);
    }

    /** Stream a dump with 1..256 concurrent requests; occupied players are logged and skipped. */
    CompletableFuture<TransferStats> importFrom(Path file, int parallelism);

    enum Type {
        ALL(),
        ONLY_BLOBS(),
        ONLY_MAILBOX();
    }
}
