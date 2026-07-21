package dev.by1337.sync;

import dev.by1337.sync.bukkit.BSync;
import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.storage.BSyncStorage;
import dev.by1337.sync.storage.FilePlayerDataStorage;
import org.bukkit.plugin.Plugin;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

public interface PlayerDataRepository<T> {
    // local://<storage-name>
    // group://group/<storage-name>
    // server://id/<storage-name>
    static <T> PlayerDataRepository<T> create(String repo, Plugin plugin, DataManager<T> dataManager) {
        if (repo.startsWith("local://")) {
            return new PlayerDataRepositoryImpl<>(
                    new FilePlayerDataStorage(new File("./bsync/" + plugin.getName() + "/" + repo.substring("local://".length()))),
                    plugin,
                    dataManager
            );
        } else if (repo.startsWith("group://")) {
            String[] args = repo.substring("group://".length()).split("/", 2);
            if (args.length != 2)
                throw new RuntimeException("Bad storage address! \"" + repo + "\" use \"group://group/<storage-name>\"");
            var list = BSync.getGroup(args[0]);
            BSyncStorage bss = new BSyncStorage();
            ChannelMaker.ChannelData<Locks> locks;
            bss.setLocks(locks = ChannelMaker.createGroupLocks(list, args[1], bss.asBSyncLockManager()));
            //try to wait to connect
            int x = 50;
            while (x-- > 0 && !locks.get().isReady()){
                LockSupport.parkNanos(2_000_000);
            }
            return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
        } else if (repo.startsWith("server://")) {
            String[] args = repo.substring("server://".length()).split("/", 2);
            if (args.length != 2)
                throw new RuntimeException("Bad storage address! \"" + repo + "\" use \"server://id/<storage-name>\"");
            var conn = BSync.getConnection(args[0]);
            BSyncStorage bss = new BSyncStorage();
            ChannelMaker.ChannelData<Locks> locks;
            bss.setLocks(locks = ChannelMaker.createLocks(conn, args[1], bss.asBSyncLockManager()));
            int x = 50;
            while (x-- > 0 && !locks.get().isReady()){
                LockSupport.parkNanos(2_000_000);
            }
            return new PlayerDataRepositoryImpl<>(bss, plugin, dataManager);
        } else {
            throw new IllegalArgumentException("Unknown storage address! \"" + repo + "\" use \"local://<storage-name>\", \"group://group/<storage-name>\", \"server://id/<storage-name>\"");
        }
    }

    void close();

    @Nullable T getUser(UUID key);

    void pushMail(UUID key, String mail);

    void pushSnapshot(UUID key, T user);
}
