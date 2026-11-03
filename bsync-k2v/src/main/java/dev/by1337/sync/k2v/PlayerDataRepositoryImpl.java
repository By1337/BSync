package dev.by1337.sync.k2v;

import dev.by1337.sync.k2v.login.ConfigurationPacketLoginHook;
import dev.by1337.sync.k2v.storage.PlayerDataStorage;
import dev.by1337.sync.k2v.transfer.RepositoryTransfer;
import dev.by1337.sync.k2v.transfer.TransferStats;
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
import org.bukkit.scheduler.BukkitTask;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class PlayerDataRepositoryImpl<T> implements Listener, PlayerDataRepository<T> {
    private static final BooleanSupplier UNTRACKED_CONNECTION = () -> true;
    private static final Logger log = LoggerFactory.getLogger(PlayerDataRepositoryImpl.class);
    private final Plugin plugin;
    private final Map<UUID, Wrapped<T>> users;
    private final PlayerDataStorage storage;
    private final DataManager<T> dataManager;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final Listener loginHook;
    private BukkitTask pendingLoginCleanup;

    public PlayerDataRepositoryImpl(PlayerDataStorage storage, Plugin plugin, DataManager<T> dataManager) {
        this.storage = storage;
        this.plugin = plugin;
        this.dataManager = dataManager;
        users = new ConcurrentHashMap<>();
        storage.setMailHandler(mail -> onMail(mail.owner(), mail.payload(), mail.id()));
        storage.setLockValidator(users::containsKey);
        storage.setLockLostHandler(this::onLockLost);

        if (plugin != null) {
            plugin.getServer().getPluginManager().registerEvents(this, plugin);
            if (hasClass("com.by1337.leaf.event.player.AsyncPlayerDataLoadEvent")) {
                loginHook = new AsyncLoadLoginHook();
            } else if (hasClass("io.papermc.paper.event.connection.configuration.AsyncPlayerConnectionConfigureEvent")) {
                loginHook = new ConfigurationLoginHook();
            } else if (ConfigurationPacketLoginHook.isSupported()) {
                loginHook = new ConfigurationPacketLoginHook(plugin,
                        (key, connected) -> loadDataAndLock(key, connected) != null);
            } else {
                loginHook = new DefaultLoginHook();
                log.warn("No post-login async hook available: proxy transfers may fail on this Paper version.");
            }
            plugin.getServer().getPluginManager().registerEvents(loginHook, plugin);
            pendingLoginCleanup = plugin.getServer().getScheduler().runTaskTimerAsynchronously(plugin, () -> {
                for (var entry : users.entrySet()) {
                    var value = entry.getValue();
                    if (value.joinTime == 0 && (!value.connected.getAsBoolean()
                            || (value.connected == UNTRACKED_CONNECTION
                                    && System.nanoTime() - value.createTime > TimeUnit.SECONDS.toNanos(60)))) {
                        removeData(entry.getKey(), value);
                    }
                }
            }, 20L, 20L);
            for (Player player : Bukkit.getOnlinePlayers()) {
                plugin.getServer().getScheduler().runTaskAsynchronously(plugin, () -> {
                    var data = loadDataAndLock(player.getUniqueId(), player::isOnline);
                    if (closing.get()) return;
                    if (data != null) {
                        var loaded = users.get(player.getUniqueId());
                        if (loaded != null) loaded.joinTime = System.currentTimeMillis();
                        storage.doMailsLoad(player.getUniqueId());
                    } else if (player.isOnline()) {
                        plugin.getServer().getScheduler().runTask(plugin, () -> {
                            if (!player.isOnline() || closing.get()) return;
                            try {
                                dataManager.forceUnlock(player.getUniqueId());
                            } catch (Exception e) {
                                log.error("Failed to forceUnlock {}", player.getUniqueId(), e);
                            }
                            player.kick(Component.text("Failed to load player data!"));
                        });
                    }
                });
            }
        } else {
            // else in test?
            loginHook = null;
        }
    }

    private void onMail(UUID key, String mail, UUID messageId) {
        T user = getUser(key);
        if (user == null) {
            throw new IllegalStateException("Mail for unloaded player " + key);
        }
        try {
            dataManager.acceptMail(user, mail, key, messageId);
        } catch (Exception e) {
            log.error("Failed to accept mail id={} owner={}", messageId, key, e);
            throw new IllegalStateException("Mail handler failed: " + messageId, e);
        }
    }

    private void onLockLost(UUID key, int version) {
        var value = users.get(key);
        if (value == null) return;
        synchronized (value) {
            if (users.get(key) != value || (version != -1 && value.version != version) || value.lost) return;
            value.lost = true;
        }
        // Keep the reservation until Quit/failed login: a new local login must not
        // replace this session while the old player is still being disconnected.
        Runnable disconnect = () -> {
            if (users.get(key) != value) return;
            try {
                dataManager.forceUnlock(key);
            } catch (Exception e) {
                log.error("Failed to notify lock loss for {}", key, e);
            }
            if (plugin != null) {
                Player player = plugin.getServer().getPlayer(key);
                if (player != null) player.kick(Component.text("Player data lease was lost. Please reconnect."));
            }
        };
        if (plugin == null) disconnect.run();
        else if (!closing.get()) plugin.getServer().getScheduler().runTask(plugin, disconnect);
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) return;
        if (plugin != null) {
            if (pendingLoginCleanup != null) pendingLoginCleanup.cancel();
            if (loginHook instanceof ConfigurationPacketLoginHook hook) hook.close();
            HandlerList.unregisterAll(this);
            if (loginHook != null) {
                HandlerList.unregisterAll(loginHook);
            }
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
        storage.close();
    }

    @Override
    public @Nullable T getUser(UUID key) {
        T value = Wrapped.unwrap(users.get(key));
        return value != null && storage.isLocked(key) ? value : null;
    }

    @Override
    public void pushMail(UUID key, String mail) {
        if (closing.get()) throw new IllegalStateException("Repository is closed");
        storage.pushMail(key, mail);
    }

    @Override
    public CompletableFuture<TransferStats> exportTo(Path file) {
        if (closing.get()) return CompletableFuture.failedFuture(new IllegalStateException("Repository is closed"));
        return RepositoryTransfer.exportTo(storage, file);
    }

    @Override
    public CompletableFuture<TransferStats> importFrom(Path file, int parallelism) {
        if (closing.get()) return CompletableFuture.failedFuture(new IllegalStateException("Repository is closed"));
        return RepositoryTransfer.importFrom(storage, file, parallelism);
    }

    @Override
    public void pushSnapshot(UUID key, T user) {
        if (users.containsKey(key)) {
            var value = users.get(key);
            if (value == null || value.lost) return;
            value.val = user;
            if (closing.get()) return; //no write
            write(key, user);
        } else {
            log.error("Trying to push snapshot for unlocked key {}", key);
        }
    }

    @Override
    public CompletableFuture<@Nullable T> loadSnapshot(UUID key) {
        return storage.loadSnapshot(key).thenApply(snapshot -> snapshot == null ? null : dataManager.read(snapshot, key));
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
        if (v != null) {
            users.remove(key, v);
            storage.unlock(key);
        }
    }

    private void write(UUID key, T data) {
        try {
            byte[] snapshot = dataManager.write(data, key);
            storage.pushSnapshot(key, snapshot);
        } catch (Exception e) {
            log.error("Failed to write player data {}", key, e);
        }
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onJoin(PlayerJoinEvent event) {
        var player = event.getPlayer();
        if (!player.isOnline()) return;
        var v = users.get(player.getUniqueId());
        if (v == null || v.val == null || v.lost) {
            player.kick(Component.text("Failed to load player data!"));
        } else {
            v.joinTime = System.currentTimeMillis();
            storage.doMailsLoad(player.getUniqueId());
        }
    }

    private void removeData(UUID key, Wrapped<T> value) {
        synchronized (value) {
            if (users.get(key) != value) return;
            if (value.val != null && !value.lost) write(key, value.val);
            users.remove(key, value);
            storage.unlock(key, value.version);
        }
    }

    private @Nullable T loadDataAndLock(UUID key) {
        var old = users.get(key);
        // Legacy hooks cannot identify the pending connection. Preserve their stale-login
        // recovery without expiring a live configuration connection on supported Paper.
        if (old != null && old.connected == UNTRACKED_CONNECTION && old.joinTime == 0
                && System.nanoTime() - old.createTime > TimeUnit.SECONDS.toNanos(5)) {
            removeData(key, old);
        }
        return loadDataAndLock(key, UNTRACKED_CONNECTION);
    }

    private @Nullable T loadDataAndLock(UUID key, BooleanSupplier connected) {
        if (closing.get() || !connected.getAsBoolean()) return null;
        Wrapped<T> pending = new Wrapped<>(null, connected);
        if (users.putIfAbsent(key, pending) != null) return null;
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            synchronized (pending) {
                pending.version = storage.lockAndLoadData(key, (locked, payload) -> {
                    synchronized (pending) {
                        if (future.isDone() || users.get(key) != pending || pending.lost || closing.get()
                                || !connected.getAsBoolean()) {
                            future.complete(null);
                            return;
                        }
                        if (!locked) {
                            future.complete(null);
                            return;
                        }
                        try {
                            pending.val = dataManager.read(payload, key);
                            future.complete(pending.val);
                        } catch (Exception e) {
                            log.error("Failed to deserialize player data {}", key, e);
                            future.completeExceptionally(e);
                        }
                    }
                });
            }
            T data = future.get(30, TimeUnit.SECONDS);
            synchronized (pending) {
                if (data != null && users.get(key) == pending && !pending.lost && !closing.get()
                        && connected.getAsBoolean() && storage.isLocked(key))
                    return data;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted loading player data {}", key, e);
        } catch (Exception e) {
            log.error("Failed to load player data {}", key, e);
        }
        synchronized (pending) {
            future.complete(null);
            if (users.remove(key, pending)) storage.unlock(key, pending.version);
        }
        return null;
    }

    private static class Wrapped<T> {
        private volatile T val;
        private volatile boolean lost;
        private volatile long joinTime;
        private int version;
        private final BooleanSupplier connected;
        private final long createTime = System.nanoTime();

        private Wrapped(T val, BooleanSupplier connected) {
            this.val = val;
            this.connected = connected;
        }

        public static <T> @Nullable T unwrap(@Nullable Wrapped<T> w) {
            if (w == null) return null;
            return w.lost ? null : w.val;
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

    private class DefaultLoginHook implements Listener {
        private final Map<AsyncPlayerPreLoginEvent, Wrapped<T>> attempts = new ConcurrentHashMap<>();

        @EventHandler(priority = EventPriority.MONITOR)
        public void onLoginMonitor(AsyncPlayerPreLoginEvent event) {
            var attempt = attempts.remove(event);
            if (attempt != null && event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
                removeData(event.getUniqueId(), attempt);
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
            } else {
                var attempt = users.get(event.getUniqueId());
                if (attempt != null) attempts.put(event, attempt);
            }
        }
    }

    private class ConfigurationLoginHook implements Listener {
        @EventHandler(priority = EventPriority.HIGH)
        public void onConfigure(io.papermc.paper.event.connection.configuration.AsyncPlayerConnectionConfigureEvent event) {
            var connection = event.getConnection();
            BooleanSupplier connected;
            try {
                connected = ConfigurationPacketLoginHook.configurationConnected(connection);
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.error("Failed to inspect configuring connection", e);
                connection.disconnect(Component.text("Failed to load player data!"));
                return;
            }
            if (!connected.getAsBoolean()) return;
            UUID key = connection.getProfile().getId();
            if (closing.get()) {
                connection.disconnect(Component.text("Failed to load player data!"));
                return;
            }
            var data = loadDataAndLock(key, connected);
            if (data == null) {
                connection.disconnect(Component.text("Failed to load player data!"));
            }
        }
    }

    private class AsyncLoadLoginHook implements Listener {
        @EventHandler(ignoreCancelled = true, priority = EventPriority.HIGH)
        public void onLogin(com.by1337.leaf.event.player.AsyncPlayerDataLoadEvent event) {
            if (event.isCancelled()) return;
            if (closing.get()) {
                event.setCancelled(true);
                return;
            }
            var data = loadDataAndLock(event.player());
            if (data == null) {
                event.setCancelled(true);
            }
        }
    }
}
