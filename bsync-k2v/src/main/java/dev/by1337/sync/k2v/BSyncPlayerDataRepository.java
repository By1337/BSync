package dev.by1337.sync.k2v;

import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.k2v.storage.BSyncStorage;
import org.bukkit.plugin.Plugin;

import java.util.UUID;
import java.util.function.Consumer;

public interface BSyncPlayerDataRepository {

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
