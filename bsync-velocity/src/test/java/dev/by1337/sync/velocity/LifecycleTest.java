package dev.by1337.sync.velocity;

import dev.by1337.sync.client.network.Connection;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LifecycleTest {
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
