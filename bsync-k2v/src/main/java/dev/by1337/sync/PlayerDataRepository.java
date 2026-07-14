package dev.by1337.sync;

import org.bukkit.plugin.Plugin;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.UUID;

public interface PlayerDataRepository<T> {
    static <T> PlayerDataRepository<T> create(String repo, Plugin plugin, DataManager<T> dataManager) {
        //todo bsync
        return new FilePlayerDataRepository<>(
                new File("./bsync./" + plugin.getName() + "/" + repo),
                plugin,
                dataManager
        );
    }

    void close();

    @Nullable T getUser(UUID key);

    void pushMail(UUID key, String mail);

    void pushSnapshot(UUID key, T user);
}
