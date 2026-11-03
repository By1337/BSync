package dev.by1337.sync.k2v.login;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bukkit.Server;
import org.bukkit.entity.Player;
import org.bukkit.event.player.PlayerLoginEvent;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitScheduler;
import org.bukkit.scheduler.BukkitTask;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfigurationPacketLoginHookTest {
    @Test
    void modernConnectionUsesPublicStatusWithoutReadingInternalFields() throws Exception {
        var connection = new ModernPaperConnection();
        var connected = ConfigurationPacketLoginHook.configurationConnected(connection);
        assertTrue(connected.getAsBoolean());
        connection.active.set(false);
        assertFalse(connected.getAsBoolean());
    }

    @Test
    void failingPublicStatusDoesNotKeepAnUnknownConnectionAlive() throws Exception {
        Object connection = new Object() {
            public boolean isConnected() {
                throw new IllegalStateException("disconnected");
            }
        };
        assertFalse(ConfigurationPacketLoginHook.configurationConnected(connection).getAsBoolean());
    }

    private static class ModernPaperConnection {
        private final AtomicBoolean active = new AtomicBoolean(true);

        public boolean isConnected() {
            return active.get();
        }
    }

    @Test
    void loginOnlyInstallsGateAndDataLoadsAfterConfigurationOnScheduler() {
        var channel = new EmbeddedChannel();
        channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());
        var workerTask = new AtomicReference<Runnable>();
        var calls = new AtomicInteger();
        Plugin plugin = plugin(workerTask);
        var key = UUID.randomUUID();
        var hook = new ConfigurationPacketLoginHook(plugin, (uuid, connected) -> {
            assertEquals(key, uuid);
            assertTrue(connected.getAsBoolean());
            calls.incrementAndGet();
            return true;
        });
        try {
            hook.onLogin(login(channel, key));
            channel.runPendingTasks();
            assertEquals(0, calls.get());
            assertNull(workerTask.get());
            var finish = new ServerboundFinishConfigurationPacket();
            assertFalse(channel.writeInbound(finish));
            assertEquals(0, calls.get());
            assertNotNull(workerTask.get());
            workerTask.get().run();
            channel.runPendingTasks();
            assertEquals(1, calls.get());
            assertSame(finish, channel.readInbound());
            hook.close();
            channel.runPendingTasks();
            assertTrue(channel.isActive(), "Closing the repository must not close a joined connection");
        } finally {
            hook.close();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shutdownClosesHeldLoginAndQueuedWorkerSeesCancelledConnection() {
        var channel = new EmbeddedChannel();
        channel.pipeline().addLast("packet_handler", new ChannelInboundHandlerAdapter());
        var workerTask = new AtomicReference<Runnable>();
        var hook = new ConfigurationPacketLoginHook(plugin(workerTask), (uuid, connected) -> {
            assertFalse(connected.getAsBoolean());
            return false;
        });
        try {
            hook.onLogin(login(channel, UUID.randomUUID()));
            channel.runPendingTasks();
            channel.writeInbound(new ServerboundFinishConfigurationPacket());
            hook.close();
            channel.runPendingTasks();
            assertFalse(channel.isActive());
            workerTask.get().run();
            channel.runPendingTasks();
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void configurationConnectionStatusUsesChannelWithoutNewPaperApiMethods() throws Exception {
        var channel = new EmbeddedChannel();
        try {
            var connected = ConfigurationPacketLoginHook.configurationConnected(new PaperConnection(channel));
            assertTrue(connected.getAsBoolean());
            channel.close();
            assertFalse(connected.getAsBoolean());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static Plugin plugin(AtomicReference<Runnable> workerTask) {
        Plugin plugin = mock(Plugin.class);
        Server server = mock(Server.class);
        BukkitScheduler scheduler = mock(BukkitScheduler.class);
        when(plugin.getServer()).thenReturn(server);
        when(server.getScheduler()).thenReturn(scheduler);
        when(scheduler.runTaskAsynchronously(eq(plugin), any(Runnable.class))).thenAnswer(call -> {
            workerTask.set(call.getArgument(1));
            return mock(BukkitTask.class);
        });
        return plugin;
    }

    private static PlayerLoginEvent login(Channel channel, UUID key) {
        Player player = mock(Player.class, withSettings().extraInterfaces(HandleProvider.class));
        when(((HandleProvider) player).getHandle()).thenReturn(new Handle(channel));
        when(player.getUniqueId()).thenReturn(key);
        return new PlayerLoginEvent(player, "localhost", InetAddress.getLoopbackAddress());
    }

    public interface HandleProvider {
        Handle getHandle();
    }

    public static class Handle {
        public final LoginListener transferCookieConnection;

        Handle(Channel channel) {
            transferCookieConnection = new LoginListener(channel);
        }
    }

    public static class LoginListener {
        public final NetworkConnection connection;

        LoginListener(Channel channel) {
            connection = new NetworkConnection(channel);
        }
    }

    public static class NetworkConnection {
        public final Channel channel;

        NetworkConnection(Channel channel) {
            this.channel = channel;
        }
    }

    private static class PaperConnection {
        private final LoginListener handle;

        PaperConnection(Channel channel) {
            handle = new LoginListener(channel);
        }
    }

    private static class ServerboundFinishConfigurationPacket {
    }
}
