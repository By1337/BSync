package dev.by1337.sync.velocity;

import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.Connection;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LifecycleTest {
    @Test
    void groupLookupCombinesAliasesWithoutDuplicatesAndKeepsDirectLookup() throws Exception {
        var firstConfig = new ConnectionConfig("players", "localhost", 8013, "test-key-1");
        var secondConfig = new ConnectionConfig("economy", "localhost", 8014, "test-key-2");
        var thirdConfig = new ConnectionConfig("other", "localhost", 8015, "test-key-3");
        var first = mock(Connection.class);
        var second = mock(Connection.class);
        var third = mock(Connection.class);
        when(first.config()).thenReturn(firstConfig);
        when(second.config()).thenReturn(secondConfig);
        when(third.config()).thenReturn(thirdConfig);
        var config = new Config("proxy-1", 1, -1,
                Map.of("first", firstConfig, "second", secondConfig, "third", thirdConfig),
                Map.of("primary", "first"),
                Map.of("all_data", List.of("players", "economy", "players")));
        var plugin = new BSync(mock(Logger.class), Path.of("."));
        var configField = BSync.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(plugin, config);
        var connectionsField = BSync.class.getDeclaredField("connections");
        connectionsField.setAccessible(true);
        connectionsField.set(plugin, Map.of("first", first, "second", second, "third", third));
        var instanceField = BSync.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        Object previous = instanceField.get(null);
        instanceField.set(null, plugin);
        try {
            var combined = BSync.getGroup("all_data");
            assertEquals(2, combined.size());
            assertTrue(combined.containsAll(List.of(first, second)));
            assertEquals(List.of(first), BSync.getGroup("players"));
            assertThrows(IllegalStateException.class, () -> BSync.getGroup("missing"));
            assertSame(first, BSync.getConnection("primary"));
            assertEquals(3, BSync.connections().size());
        } finally {
            instanceField.set(null, previous);
        }
    }

    @Test
    void shutdownWaitsForEveryConnectionWithoutBlockingTheCallingThread() {
        var first = mock(Connection.class);
        var second = mock(Connection.class);
        var a = new CompletableFuture<Void>();
        var b = new CompletableFuture<Void>();
        when(first.close()).thenReturn(a);
        when(second.close()).thenReturn(b);
        var closed = BSync.closeConnections(List.of(first, second));
        assertFalse(closed.isDone());
        a.complete(null);
        assertFalse(closed.isDone());
        b.complete(null);
        assertTrue(closed.isDone());
        verify(first).close();
        verify(second).close();
    }
}
