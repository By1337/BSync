package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.handler.lock.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.RequestMsg;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.packet.impl.a2a.A2ALongResponse;
import dev.by1337.sync.common.packet.impl.c2s.C2SGiveMailUidsRequest;
import dev.by1337.sync.common.packet.impl.c2s.C2SPushMailPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ClientMailPushTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("mail-push-test");
    private final UUID player = new UUID(13, 37);
    private final BlockingQueue<RequestMsg<?>> requests = new LinkedBlockingQueue<>();
    private ClientLocksHandler handler;
    private Pipeline pipeline;

    @BeforeEach
    void setUp() throws Exception {
        run(() -> {
            pipeline = new Pipeline(WORKER).addFirst("capture", new ChannelHandler() {
                public void init(ChannelRuntime runtime) {}
                public void close() {}
                public void handle(ChannelContext ctx, ChannelMessage msg) {
                    if (msg instanceof RequestMsg<?> request) requests.add(request);
                    else ctx.fire(msg);
                }
            });
            handler = new ClientLocksHandler(Locks.Type.ONLY_MAILBOX);
            handler.lockManager(new LockManager() {
                public boolean ensureLockOwnership(UUID key) { return true; }
                public void acceptMail(UUID key, String json) {}
                public void forceUnlock(UUID key) {}
                public void close() {}
            });
            pipeline.addLast("locks", handler);
            handler.init(new ClientChannelRuntime() {
                public Connection remote() { return pipeline.local(); }
                public Pipeline pipeline() { return pipeline; }
                public EventLoopWorker eventLoop() { return WORKER; }
                public Logger logger() { return LoggerFactory.getLogger(ClientMailPushTest.class); }
            });
            pipeline.execute(ChannelActiveMessage.INSTANCE, pipeline.local());
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        pipeline.closeAll().get(5, TimeUnit.SECONDS);
    }

    @Test
    void mailWaitsForOffsetAndIdsIncreaseWithinAllocatedRange() throws Exception {
        var range = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            handler.pushMail(player, "first");
            handler.pushMail(player, "second");
            assertTrue(requests.isEmpty());
            complete(range, new A2ALongResponse(100));
        });
        var first = take(C2SPushMailPacket.class);
        var second = take(C2SPushMailPacket.class);
        assertEquals(100, ((C2SPushMailPacket) first.packet()).uid());
        assertEquals(101, ((C2SPushMailPacket) second.packet()).uid());
        run(() -> {
            complete(first, new A2AFlagResponse(true));
            complete(second, new A2AFlagResponse(true));
        });
        assertNull(requests.poll(2200, TimeUnit.MILLISECONDS));
    }

    @Test
    void exhaustedRangeWaitsForNextOffsetWithoutReusingIds() throws Exception {
        var range = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            complete(range, new A2ALongResponse(100));
            set("nextMailUid", 999);
            set("remainingMailUids", 1);
            handler.pushMail(player, "last");
            handler.pushMail(player, "next");
        });
        var last = take(C2SPushMailPacket.class);
        var refill = take(C2SGiveMailUidsRequest.class);
        assertEquals(999, ((C2SPushMailPacket) last.packet()).uid());
        run(() -> {
            complete(last, new A2AFlagResponse(true));
            complete(refill, new A2ALongResponse(2000));
        });
        var next = take(C2SPushMailPacket.class);
        assertEquals(2000, ((C2SPushMailPacket) next.packet()).uid());
        run(() -> complete(next, new A2AFlagResponse(true)));
    }

    @Test
    void falseResponseRetriesSamePacketAndStopsOnTrue() throws Exception {
        var range = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            complete(range, new A2ALongResponse(100));
            handler.pushMail(player, "mail");
        });
        var first = take(C2SPushMailPacket.class);
        run(() -> complete(first, new A2AFlagResponse(false)));
        var retry = take(C2SPushMailPacket.class);
        assertSame(first.packet(), retry.packet());
        run(() -> complete(retry, new A2AFlagResponse(true)));
        assertNull(requests.poll(2200, TimeUnit.MILLISECONDS));
    }

    @Test
    void missingOffsetResponseRetriesWithoutRandomFallback() throws Exception {
        var range = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            handler.pushMail(player, "mail");
            complete(range, null);
        });
        var retry = take(C2SGiveMailUidsRequest.class);
        assertTrue(requests.isEmpty());
        run(() -> complete(retry, new A2ALongResponse(700)));
        var push = take(C2SPushMailPacket.class);
        assertEquals(700, ((C2SPushMailPacket) push.packet()).uid());
        run(() -> complete(push, new A2AFlagResponse(true)));
    }

    @Test
    void reconnectDiscardsOldRangeResponseAndRequestsFreshOffset() throws Exception {
        var oldRange = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            pipeline.execute(ChannelInactiveMessage.INSTANCE, pipeline.local());
            pipeline.execute(ChannelActiveMessage.INSTANCE, pipeline.local());
            handler.pushMail(player, "new");
            complete(oldRange, new A2ALongResponse(100));
        });
        var range = take(C2SGiveMailUidsRequest.class);
        assertTrue(requests.isEmpty());
        run(() -> complete(range, new A2ALongResponse(800)));
        var push = take(C2SPushMailPacket.class);
        assertEquals(800, ((C2SPushMailPacket) push.packet()).uid());
        run(() -> complete(push, new A2AFlagResponse(true)));
    }

    @Test
    void scheduledRetryDoesNotSendOldMailAfterReconnect() throws Exception {
        var range = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            complete(range, new A2ALongResponse(100));
            handler.pushMail(player, "old");
        });
        var push = take(C2SPushMailPacket.class);
        run(() -> {
            complete(push, null);
            pipeline.execute(ChannelInactiveMessage.INSTANCE, pipeline.local());
            pipeline.execute(ChannelActiveMessage.INSTANCE, pipeline.local());
        });
        var newRange = take(C2SGiveMailUidsRequest.class);
        run(() -> complete(newRange, new A2ALongResponse(1000)));
        assertNull(requests.poll(2200, TimeUnit.MILLISECONDS));
    }

    @Test
    void retryLimitAndDeadlineStopSending() throws Exception {
        var range = take(C2SGiveMailUidsRequest.class);
        run(() -> {
            complete(range, new A2ALongResponse(100));
            var method = ClientLocksHandler.class.getDeclaredMethod(
                    "sendMail", C2SPushMailPacket.class, int.class, int.class, long.class);
            method.setAccessible(true);
            method.invoke(handler, new C2SPushMailPacket(player, "last attempt", 100),
                    10, 0, System.nanoTime() + 120_000_000_000L);
            method.invoke(handler, new C2SPushMailPacket(player, "expired", 101),
                    0, 0, System.nanoTime() - 1);
        });
        var last = take(C2SPushMailPacket.class);
        run(() -> complete(last, null));
        assertNull(requests.poll(2200, TimeUnit.MILLISECONDS));
    }

    private RequestMsg<?> take(Class<?> type) throws Exception {
        var request = requests.poll(5, TimeUnit.SECONDS);
        assertNotNull(request, "Expected " + type.getSimpleName());
        assertInstanceOf(type, request.packet());
        return request;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void complete(RequestMsg<?> request, Object response) {
        ((dev.by1337.sync.common.callback.ResponseFuture) request.consumer()).complete(response);
    }

    private void set(String name, Object value) throws Exception {
        var field = ClientLocksHandler.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(handler, value);
    }

    private static void run(CheckedRunnable action) throws Exception {
        var done = new CompletableFuture<Void>();
        WORKER.execute(() -> {
            try {
                action.run();
                done.complete(null);
            } catch (Throwable error) {
                done.completeExceptionally(error);
            }
        });
        done.get(5, TimeUnit.SECONDS);
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
