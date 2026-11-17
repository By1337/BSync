package dev.by1337.sync.client.network;

import dev.by1337.sync.client.channel.ClientChannel;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelContext;
import dev.by1337.sync.common.channel.pipeline.ChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.testng.annotations.Test;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalChannel;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.*;

public class ChannelRemovalTest {
    private static final EventLoopWorkers WORKERS = new EventLoopWorkers("removal-test-%s", 2);

    private Connection connection() {
        return new Connection(new ConnectionConfig("test", "localhost", 1, "test"), WORKERS, "test", null);
    }

    private ClientChannel channel(Connection connection, String id, Runnable cleanup) {
        return connection.addChannel(id, "test", channel -> channel.pipeline().addLast("test", new ChannelHandler() {
            public void init(ChannelRuntime runtime) {
            }

            public void handle(ChannelContext context, ChannelMessage message) {
                context.fire(message);
            }

            public void close() {
                cleanup.run();
            }
        }));
    }

    @Test(timeOut = 5000)
    public void recursiveRemovalClosesEveryHandlerOnceAndDrainsCleanupTasks() throws Exception {
        Connection connection = connection();
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger drained = new AtomicInteger();
        ClientChannel channel = channel(connection, "recursive", () -> {
            calls.incrementAndGet();
            connection.removeChannel("recursive");
        });
        channel.pipeline().addLast("tail", new ChannelHandler() {
            public void init(ChannelRuntime runtime) {
            }

            public void handle(ChannelContext context, ChannelMessage message) {
            }

            public void close() {
                calls.incrementAndGet();
                channel.eventLoop().executeReliably(drained::incrementAndGet);
            }
        });
        connection.removeChannel("recursive");
        assertEquals(calls.get(), 2);
        assertEquals(drained.get(), 1);
        connection.removeChannel("recursive");
        channel.close().get(1, TimeUnit.SECONDS);
        assertEquals(calls.get(), 2);
        // Removal must also release the ID and packet registry for a replacement.
        channel(connection, "recursive", () -> {
        }).close().get(1, TimeUnit.SECONDS);
        connection.close().get(1, TimeUnit.SECONDS);
    }

    @Test(timeOut = 5000)
    public void concurrentExternalRemovalsWaitForTheSameCleanup() throws Exception {
        Connection connection = connection();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), secondStarted = new CountDownLatch(1);
        channel(connection, "concurrent", () -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                assertTrue(release.await(3, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> connection.removeChannel("concurrent"));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            Future<?> second = executor.submit(() -> {
                secondStarted.countDown();
                connection.removeChannel("concurrent");
            });
            assertTrue(secondStarted.await(1, TimeUnit.SECONDS));
            try {
                second.get(100, TimeUnit.MILLISECONDS);
                fail("Concurrent removal returned before handlers finished");
            } catch (TimeoutException expected) {
            }
            release.countDown();
            first.get(1, TimeUnit.SECONDS);
            second.get(1, TimeUnit.SECONDS);
            assertEquals(calls.get(), 1);
        } finally {
            release.countDown();
            executor.shutdownNow();
            connection.close().get(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 5000)
    public void closingDisconnectedConnectionStillClosesAllHandlers() throws Exception {
        Connection connection = connection();
        AtomicInteger calls = new AtomicInteger();
        ClientChannel first = channel(connection, "first", () -> {
            calls.incrementAndGet();
            connection.removeChannel("first");
        });
        channel(connection, "second", calls::incrementAndGet);
        CompletableFuture<Void> closed = connection.close();
        assertSame(connection.close(), closed);
        closed.get(1, TimeUnit.SECONDS);
        assertEquals(calls.get(), 2);
        assertSame(first.close(), first.close());
        first.close().get(1, TimeUnit.SECONDS);
        assertEquals(calls.get(), 2);
    }

    @Test(timeOut = 5000)
    public void synchronizedOwnerCloseCannotBlockItsOwnCleanupCallback() throws Exception {
        Connection connection = connection();
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger tailCalls = new AtomicInteger();
        class MisusedHandler implements ChannelHandler {
            private boolean closed;

            public void init(ChannelRuntime runtime) {
            }

            public void handle(ChannelContext context, ChannelMessage message) {
            }

            public synchronized void close() {
                calls.incrementAndGet();
                if (closed) return;
                closed = true;
                connection.removeChannel("monitor");
            }
        }
        MisusedHandler handler = new MisusedHandler();
        ClientChannel channel = channel(connection, "monitor", tailCalls::incrementAndGet);
        channel.pipeline().addFirst("misused", handler);
        CompletableFuture.runAsync(handler::close).get(1, TimeUnit.SECONDS);
        connection.removeChannelAsync("monitor").get(1, TimeUnit.SECONDS);
        assertEquals(calls.get(), 2); // Manual owner call and exactly one BSync callback.
        assertEquals(tailCalls.get(), 1); // No handlers were skipped to avoid the lock.
        connection.close().get(1, TimeUnit.SECONDS);
    }

    @Test(timeOut = 5000)
    public void transportEventLoopNeverWaitsForItsOwnDetachTask() throws Exception {
        Connection connection = connection();
        AtomicInteger calls = new AtomicInteger();
        channel(connection, "transport", calls::incrementAndGet);
        DefaultEventLoopGroup group = new DefaultEventLoopGroup(1);
        LocalChannel socket = new LocalChannel();
        try {
            group.register(socket).sync();
            ConnectionHandler transport = new ConnectionHandler("test", connection.config(), null, connection) {
                public io.netty.channel.Channel channel() {
                    return socket;
                }
            };
            var field = Connection.class.getDeclaredField("connection");
            field.setAccessible(true);
            field.set(connection, transport);
            socket.eventLoop().submit(() -> connection.removeChannel("transport")).get(1, TimeUnit.SECONDS);
            connection.removeChannel("transport");
            assertEquals(calls.get(), 1);
            field.set(connection, null);
            connection.close().get(1, TimeUnit.SECONDS);
        } finally {
            socket.close();
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        }
    }

    @Test(timeOut = 5000)
    public void removalFromChannelWorkerClosesHandlersInline() throws Exception {
        Connection connection = connection();
        AtomicInteger calls = new AtomicInteger();
        ClientChannel channel = channel(connection, "worker", calls::incrementAndGet);
        CompletableFuture<Void> done = new CompletableFuture<>();
        channel.eventLoop().execute(() -> {
            try {
                connection.removeChannel("worker");
                assertEquals(calls.get(), 1);
                done.complete(null);
            } catch (Throwable failure) {
                done.completeExceptionally(failure);
            }
        });
        done.get(1, TimeUnit.SECONDS);
        connection.close().get(1, TimeUnit.SECONDS);
    }
}
