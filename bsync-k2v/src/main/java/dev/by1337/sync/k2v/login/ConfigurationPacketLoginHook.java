package dev.by1337.sync.k2v.login;

import io.netty.channel.Channel;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerLoginEvent;
import org.bukkit.plugin.Plugin;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;

/** Compatibility hook for Paper with configuration protocol but without the async configuration event. */
public final class ConfigurationPacketLoginHook implements Listener, AutoCloseable {
    private final Plugin plugin;
    private final BiPredicate<UUID, BooleanSupplier> load;
    private final Set<Channel> channels = ConcurrentHashMap.newKeySet();
    private final String handlerName = "bsync-login-" + UUID.randomUUID();
    private volatile boolean closed;

    public ConfigurationPacketLoginHook(Plugin plugin, BiPredicate<UUID, BooleanSupplier> load) {
        this.plugin = plugin;
        this.load = load;
    }

    public static boolean isSupported() {
        try {
            Class.forName("net.minecraft.network.protocol.configuration.ServerboundFinishConfigurationPacket");
            Class.forName("net.minecraft.server.level.ServerPlayer").getField("transferCookieConnection");
            return true;
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onLogin(PlayerLoginEvent event) {
        if (event.getResult() != PlayerLoginEvent.Result.ALLOWED) return;
        if (closed) {
            event.disallow(PlayerLoginEvent.Result.KICK_OTHER, "Failed to load player data!");
            return;
        }
        try {
            Channel channel = findChannel(event.getPlayer());
            UUID key = event.getPlayer().getUniqueId();
            channels.add(channel);
            channel.closeFuture().addListener(future -> channels.remove(channel));
            // This event precedes Login Success. Installation is queued before the client
            // can acknowledge configuration; no wait on either the main or Netty thread.
            channel.eventLoop().execute(() -> {
                if (closed || !channel.isActive()) {
                    channel.close();
                    return;
                }
                try {
                    channel.pipeline().addBefore("packet_handler", handlerName,
                            new ConfigurationPacketGate(
                                    packet -> packet.getClass().getSimpleName().equals("ServerboundFinishConfigurationPacket"),
                                    () -> {
                                        CompletableFuture<Boolean> result = new CompletableFuture<>();
                                        try {
                                            plugin.getServer().getScheduler().runTaskAsynchronously(plugin, () -> {
                                                try {
                                                    result.complete(load.test(key, () -> !closed && channel.isActive()));
                                                } catch (Throwable error) {
                                                    result.completeExceptionally(error);
                                                }
                                            });
                                        } catch (Exception error) {
                                            result.completeExceptionally(error);
                                        }
                                        return result;
                                    }));
                } catch (RuntimeException error) {
                    plugin.getLogger().log(java.util.logging.Level.SEVERE, "Failed to install configuration packet gate", error);
                    channel.close();
                }
            });
        } catch (ReflectiveOperationException | RuntimeException e) {
            plugin.getLogger().log(java.util.logging.Level.SEVERE, "Failed to install async player data login hook", e);
            event.disallow(PlayerLoginEvent.Result.KICK_OTHER, "Failed to load player data!");
        }
    }

    private static Channel findChannel(Player player) throws ReflectiveOperationException {
        Object handle = player.getClass().getMethod("getHandle").invoke(player);
        // On Mojang-mapped Paper (1.20.6+) the pre-login CraftPlayer points at the login listener here.
        Object listener = field(handle, "transferCookieConnection");
        Object connection = field(listener, "connection");
        return (Channel) field(connection, "channel");
    }

    public static BooleanSupplier configurationConnected(Object paperConnection) throws ReflectiveOperationException {
        try {
            // Newer Paper exposes this through PlayerConnection. Resolve by capability
            // to keep the same artifact usable on older APIs without this method.
            Method connected = paperConnection.getClass().getMethod("isConnected");
            if (connected.getReturnType() != boolean.class) throw new NoSuchMethodException("boolean isConnected()");
            connected.setAccessible(true);
            return () -> {
                try {
                    return (boolean) connected.invoke(paperConnection);
                } catch (ReflectiveOperationException e) {
                    return false;
                }
            };
        } catch (NoSuchMethodException ignored) {
            // Paper 1.21.7/1.21.8 has the configuration event, but no public status API.
        }
        Object listener = field(paperConnection, "handle");
        Channel channel = (Channel) field(field(listener, "connection"), "channel");
        return channel::isActive;
    }

    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        for (Class<?> type = owner.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                Object value = field.get(owner);
                if (value == null) throw new IllegalStateException("Missing login field " + name);
                return value;
            } catch (NoSuchFieldException ignored) {
            }
        }
        throw new NoSuchFieldException(name);
    }

    @Override
    public void close() {
        closed = true;
        for (Channel channel : channels) {
            channel.eventLoop().execute(() -> {
                if (channel.pipeline().get(handlerName) != null) channel.close();
            });
        }
        channels.clear();
    }
}
