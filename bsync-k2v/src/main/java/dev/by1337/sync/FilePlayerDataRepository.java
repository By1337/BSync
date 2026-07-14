package dev.by1337.sync;

import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.HandlerList;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.Plugin;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FilePlayerDataRepository<T> implements Listener, PlayerDataRepository<T> {
    private static final Logger log = LoggerFactory.getLogger(FilePlayerDataRepository.class);
    private final Plugin plugin;
    private final Map<UUID, Wrapped<T>> users;
    private final FilePlayerDataStorage storage;
    private final DataManager<T> dataManager;
    private final AtomicBoolean closing = new AtomicBoolean();

    public FilePlayerDataRepository(File dataFolder, Plugin plugin, DataManager<T> dataManager) {
        this.plugin = plugin;
        this.dataManager = dataManager;
        users = new ConcurrentHashMap<>();
        storage = new FilePlayerDataStorage(dataFolder);
        if (plugin != null) {
            plugin.getServer().getPluginManager().registerEvents(this, plugin);
            if (hasClass("io.papermc.paper.event.connection.PlayerConnectionValidateLoginEvent")) {
                plugin.getServer().getPluginManager().registerEvent(
                        io.papermc.paper.event.connection.PlayerConnectionValidateLoginEvent.class,
                        this,
                        EventPriority.MONITOR,
                        (listener, event0) -> {
                            if (event0 instanceof io.papermc.paper.event.connection.PlayerConnectionValidateLoginEvent event) {
                                if (event.isAllowed()) return;
                                if (event.getConnection() instanceof io.papermc.paper.connection.PlayerLoginConnection plc) {
                                    var v = plc.getAuthenticatedProfile();
                                    if (v != null)
                                        removeData(v.getId());
                                }
                            }
                        },
                        plugin
                );
            }

            for (Player player : Bukkit.getOnlinePlayers()) {
                var v = loadDataAndLock(player.getUniqueId());
                if (v == null) {
                    try {
                        dataManager.forceUnlock(player.getUniqueId());
                    } catch (Exception e) {
                        log.error("Failed to forceUnlock");
                    }
                }
            }
        } // else in test?
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) return;
        if (plugin != null) {
            HandlerList.unregisterAll(this);
        } // else in test?
        for (UUID key : List.copyOf(users.keySet())) {
            T user = Wrapped.unwrap(users.get(key));
            if (user != null) {
                try {
                    dataManager.forceUnlock(key);
                } catch (Exception e) {
                    log.error("Failed to forceUnlock");
                }
                write(key, user);
            }
            users.remove(key);
        }
    }

    @Override
    public @Nullable T getUser(UUID key) {
        return Wrapped.unwrap(users.get(key));
    }

    @Override
    public void pushMail(UUID key, String mail) {
        T user;
        if (!closing.get() && (user = Wrapped.unwrap(users.get(key))) != null) {
            try {
                dataManager.acceptMail(user, mail);
            } catch (Exception e) {
                log.error("Failed to accept mail! {}", mail, e);
            }
        } else {
            storage.appendMail(key, mail);
        }
    }

    @Override
    public void pushSnapshot(UUID key, T user) {
        if (users.containsKey(key)) {
            users.put(key, new Wrapped<>(user));
            if (closing.get()) return; //no write
            write(key, user);
        } else {
            log.error("Trying to push snapshot for unlocked key {}", key);
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onQuit(PlayerQuitEvent event) {
        var player = event.getPlayer();
        UUID key = player.getUniqueId();
        var v = users.get(key);
        T userData = Wrapped.unwrap(v);
        if (userData != null) {
            write(key, userData);
        }
        if (v != null) users.remove(key, v);
    }

    private void write(UUID key, T data) {
        try {
            byte[] snapshot = dataManager.write(data);
            storage.write(key, snapshot);
        } catch (Exception e) {
            log.error("Failed to write player data {}", key, e);
        }
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = true)
    public void onJoin(PlayerJoinEvent event) {
        var player = event.getPlayer();
        if (!player.isOnline()) return;
        var v = users.get(player.getUniqueId());
        if (v == null) {
            player.kick(Component.text("Failed to load player data!"));
        } else {
            v.joinTime = System.currentTimeMillis();
        }
    }


    @EventHandler(priority = EventPriority.MONITOR)
    public void onLoginMonitor(AsyncPlayerPreLoginEvent event) {
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            removeData(event.getUniqueId());
        }
    }

    private void removeData(UUID key) {
        var old = users.get(key);
        if (old != null) {
            if (old.val != null) {
                write(key, old.val);
            }
            users.remove(key, old);
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onLogin(AsyncPlayerPreLoginEvent event) {
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) return;
        if (closing.get()) {
            event.kickMessage(Component.text("Failed to load player data"));
            event.setLoginResult(AsyncPlayerPreLoginEvent.Result.KICK_OTHER);
            return;
        }
        var data = loadDataAndLock(event.getUniqueId());
        if (data == null) {
            event.setLoginResult(AsyncPlayerPreLoginEvent.Result.KICK_OTHER);
            event.kickMessage(Component.text("Failed to load player data!"));
        }
    }

    private @Nullable T loadDataAndLock(UUID key) {
        var old = users.get(key);
        if (old != null) {
            var now = System.currentTimeMillis();
            if (old.joinTime == 0 && now - old.createTime > 5_000) {
                // потерялся после Login
                if (old.val != null) {
                    write(key, old.val);
                }
                users.remove(key, old);
            }
        }
        if (users.putIfAbsent(key, new Wrapped<>(null)) != null) {
            return null;
        }
        byte[] payload = storage.read(key);
        try {
            T userData = dataManager.read(payload);
            users.put(key, new Wrapped<>(userData));
            var mails = storage.readAllMailsAndDelete(key);
            for (String mail : mails) {
                try {
                    dataManager.acceptMail(userData, mail);
                } catch (Exception e) {
                    log.error("Failed to accept mail {}", mail, e);
                }
            }
            return userData;
        } catch (Exception e) {
            log.error("Failed to deserialize player data", e);
            users.remove(key);
            return null;
        }
    }

    private static class Wrapped<T> {
        private final T val;
        private long joinTime;
        private final long createTime = System.currentTimeMillis();

        private Wrapped(T val) {
            this.val = val;
        }

        public static <T> @Nullable T unwrap(@Nullable Wrapped<T> w) {
            if (w == null) return null;
            return w.val;
        }
    }

    private static boolean hasClass(String clazz) {
        try {
            Class.forName(clazz);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
