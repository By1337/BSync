package dev.by1337.sync.k2v;

import dev.by1337.sync.bukkit.BSync;
import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.k2v.storage.BSyncStorage;
import org.bukkit.plugin.Plugin;

import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/** Network-only factory; standalone repositories never resolve this adapter. */
public interface BSyncPlayerDataRepository {

    static <T> PlayerDataRepository<T> create(String repo, Plugin plugin, DataManager<T> dataManager, PlayerDataRepository.Type type) {
        if (repo.startsWith("group://")) {
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
        }
        throw new IllegalArgumentException("Expected server:// or group:// storage address");
    }

    static <T> PlayerDataRepository<T> create(Locks locks, Runnable onClose, Plugin plugin, DataManager<T> dataManager, Consumer<LockManager> c) {
        BSyncStorage bss = new BSyncStorage();
        c.accept(bss.asBSyncLockManager());
        bss.setLocks(locks, onClose);
        return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
    }

    static PlayerDataRepository<UUID> createMailBox(Locks locks, Runnable onClose, Plugin plugin, DataManager.MailBox dataManager, Consumer<LockManager> c) {
        BSyncStorage bss = new BSyncStorage();
        c.accept(bss.asBSyncLockManager());
        bss.setLocks(locks, onClose);
        return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
    }

    static <T> PlayerDataRepository<T> createOnlyData(Locks locks, Runnable onClose, Plugin plugin, DataManager.OnlyData<T> dataManager, Consumer<LockManager> c) {
        BSyncStorage bss = new BSyncStorage();
        c.accept(bss.asBSyncLockManager());
        bss.setLocks(locks, onClose);
        return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
    }
}
