package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.handler.lock.*;
import dev.by1337.sync.client.channel.status.*;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.RequestMsg;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.packet.impl.c2s.C2SPushMailPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.*;

import java.nio.file.*;
import java.util.UUID;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ClientMailPushTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("mail-push-test");
    @TempDir
    Path directory;
    private final UUID player = UUID.randomUUID();
    private final BlockingQueue<RequestMsg<?>> requests = new LinkedBlockingQueue<>();
    private ClientLocksHandler handler;
    private Pipeline pipeline;

    @BeforeEach
    void setUp() throws Exception {
        open();
    }

    private void open() throws Exception {
        run(() -> {
            pipeline = new Pipeline(WORKER).addFirst("capture", new ChannelHandler() {
                public void init(ChannelRuntime runtime) {
                }

                public void close() {
                }

                public void handle(ChannelContext ctx, ChannelMessage msg) {
                    if (msg instanceof RequestMsg<?> request) requests.add(request);
                    else ctx.fire(msg);
                }
            });
            handler = new ClientLocksHandler(Locks.Type.ONLY_MAILBOX, directory);
            handler.lockManager(new LockManager() {
                public boolean ensureLockOwnership(UUID key) {
                    return true;
                }

                public void acceptMail(UUID key, String json) {
                    fail("Must pass through durable server mailbox");
                }

                public void forceUnlock(UUID key) {
                }

                public void close() {
                }
            });
            pipeline.addLast("locks", handler);
            handler.init(new ClientChannelRuntime() {
                public Connection remote() {
                    return pipeline.local();
                }

                public Pipeline pipeline() {
                    return pipeline;
                }

                public EventLoopWorker eventLoop() {
                    return WORKER;
                }

                public Logger logger() {
                    return LoggerFactory.getLogger(ClientMailPushTest.class);
                }
            });
        });
    }

    @AfterEach
    void close() throws Exception {
        pipeline.closeAll().get(5, TimeUnit.SECONDS);
    }

    @Test
    void disconnectedPushIsDurableBeforeReturningAndReplayedAfterRestart() throws Exception {
        handler.pushMail(player, "first");
        assertEquals(1, records());
        assertTrue(requests.isEmpty());
        pipeline.closeAll().get(5, TimeUnit.SECONDS);
        open();
        active();
        var request = take();
        assertEquals("first", ((C2SPushMailPacket) request.packet()).json());
        run(() -> complete(request, true));
        assertEquals(0, records());
    }

    @Test
    void lostAckAndReconnectRetryExactlySameMessageId() throws Exception {
        active();
        handler.pushMail(player, "mail");
        var first = take();
        run(() -> {
            pipeline.execute(ChannelInactiveMessage.INSTANCE, pipeline.local());
            pipeline.execute(ChannelActiveMessage.INSTANCE, pipeline.local());
        });
        var retry = take();
        assertEquals(first.packet(), retry.packet());
        run(() -> complete(first, false)); // stale session must not overwrite the new attempt
        assertEquals(1, records());
        run(() -> complete(retry, true));
        assertEquals(0, records());
    }

    @Test
    void closeWithUnconfirmedMailKeepsStableIdForNextProcess() throws Exception {
        active();
        handler.pushMail(player, "mail");
        var first = take();
        pipeline.closeAll().get(5, TimeUnit.SECONDS);
        open();
        active();
        var retry = take();
        assertEquals(first.packet(), retry.packet());
        run(() -> complete(retry, true));
        assertEquals(0, records());
    }

    @Test
    void confirmationIsRequiredBeforeNextMailForSamePlayer() throws Exception {
        handler.pushMail(player, "first");
        handler.pushMail(player, "second");
        active();
        var first = take();
        assertEquals("first", ((C2SPushMailPacket) first.packet()).json());
        assertTrue(requests.isEmpty());
        assertEquals(2, records());
        run(() -> complete(first, true));
        var second = take();
        assertEquals("second", ((C2SPushMailPacket) second.packet()).json());
        assertNotEquals(((C2SPushMailPacket) first.packet()).uid(), ((C2SPushMailPacket) second.packet()).uid());
        run(() -> complete(second, true));
        assertEquals(0, records());
    }

    @Test
    void negativeAckRetainsRecordAndAutomaticallyRetries() throws Exception {
        active();
        handler.pushMail(player, "mail");
        var first = take();
        run(() -> complete(first, false));
        assertEquals(1, records());
        var retry = take();
        assertEquals(first.packet(), retry.packet());
        run(() -> complete(retry, true));
        assertEquals(0, records());
    }

    @Test
    void queuedBeforeFirstConnectionSurvivesCloseWithoutRuntimeInitialization() {
        Path journal = directory.resolve("never-connected");
        var neverConnected = new ClientLocksHandler(Locks.Type.ONLY_MAILBOX, journal);
        neverConnected.pushMail(player, "offline admission");
        neverConnected.close();
        try (var reopened = new dev.by1337.sync.common.mail.MailJournal(journal)) {
            assertEquals(1, reopened.pending().size());
            assertEquals("offline admission", reopened.pending().get(0).payload());
        }
    }

    private long records() throws Exception {
        try (var paths = Files.list(directory)) {
            return paths.filter(p -> p.toString().endsWith(".mail")).count();
        }
    }

    private void active() throws Exception {
        run(() -> pipeline.execute(ChannelActiveMessage.INSTANCE, pipeline.local()));
    }

    private RequestMsg<?> take() throws Exception {
        var request = requests.poll(5, TimeUnit.SECONDS);
        assertNotNull(request);
        assertInstanceOf(C2SPushMailPacket.class, request.packet());
        return request;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void complete(RequestMsg<?> request, boolean accepted) {
        ((dev.by1337.sync.common.callback.ResponseFuture) request.consumer()).complete(new A2AFlagResponse(accepted));
    }

    private static void run(CheckedRunnable action) throws Exception {
        var result = new CompletableFuture<Void>();
        WORKER.execute(() -> {
            try {
                action.run();
                result.complete(null);
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        result.get(5, TimeUnit.SECONDS);
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
