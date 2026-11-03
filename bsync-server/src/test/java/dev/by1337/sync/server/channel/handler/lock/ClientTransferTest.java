package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.handler.lock.*;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.RequestMsg;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.transfer.*;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.junit.jupiter.api.*;
import org.slf4j.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ClientTransferTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("client-transfer-test");
    private final BlockingQueue<RequestMsg<?>> requests = new LinkedBlockingQueue<>();
    private ClientLocksHandler handler;
    private Pipeline pipeline;

    @BeforeEach
    void setUp() throws Exception {
        run(() -> {
            pipeline = new Pipeline(WORKER).addFirst("capture", new ChannelHandler() {
                public void init(ChannelRuntime runtime) {
                }

                public void close() {
                }

                public void handle(ChannelContext ctx, ChannelMessage message) {
                    if (message instanceof RequestMsg<?> request) requests.add(request);
                    else ctx.fire(message);
                }
            });
            handler = new ClientLocksHandler(Locks.Type.ALL, null);
            handler.lockManager(new LockManager() {
                public boolean ensureLockOwnership(UUID key) {
                    return false;
                }

                public void acceptMail(UUID key, String json) {
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
                    return LoggerFactory.getLogger(ClientTransferTest.class);
                }
            });
            pipeline.execute(ChannelActiveMessage.INSTANCE, pipeline.local());
        });
    }

    @AfterEach
    void close() throws Exception {
        pipeline.closeAll().get(5, TimeUnit.SECONDS);
    }

    @Test
    void exportPagesUseServerCursorAndMailStartsFromItsOwnSequence() throws Exception {
        var source = handler.exportSources(new ShardSelection(7, 3)).get(0);
        UUID key = UUID.randomUUID();
        var first = source.next(TransferEntry.Kind.DATA);
        var request = take();
        assertNull(((C2SExportPagePacket) request.packet()).afterKey());
        assertEquals(new ShardSelection(7, 3), ((C2SExportPagePacket) request.packet()).shard());
        run(() -> complete(request, new S2CExportPagePacket(List.of(new TransferEntry(TransferEntry.Kind.DATA,
                key, new byte[]{ 1 }, null)), key, 0, false, "")));
        assertEquals(key, first.get().afterKey());
        var second = source.next(TransferEntry.Kind.DATA);
        var next = take();
        assertEquals(key, ((C2SExportPagePacket) next.packet()).afterKey());
        assertEquals(new ShardSelection(7, 3), ((C2SExportPagePacket) next.packet()).shard());
        run(() -> complete(next, new S2CExportPagePacket(List.of(), key, 0, true, "")));
        second.get();
        var mails = source.next(TransferEntry.Kind.MAIL);
        var mailRequest = take();
        assertEquals(0, ((C2SExportPagePacket) mailRequest.packet()).afterSequence());
        assertEquals(new ShardSelection(7, 3), ((C2SExportPagePacket) mailRequest.packet()).shard());
        run(() -> complete(mailRequest, S2CExportPagePacket.empty()));
        mails.get();
    }

    @Test
    void importReportsLocksAndPreservesMailIdInsteadOfGeneratingAnother() throws Exception {
        UUID key = UUID.randomUUID(), id = UUID.randomUUID();
        var data = handler.importEntry(new TransferEntry(TransferEntry.Kind.DATA, key, new byte[]{ 1 }, null));
        var dataRequest = take();
        assertInstanceOf(C2SImportDataPacket.class, dataRequest.packet());
        run(() -> complete(dataRequest, new S2CImportResultPacket(TransferResult.Status.LOCKED, "active player")));
        assertEquals(TransferResult.Status.LOCKED, data.get().status());
        var mail = handler.importEntry(new TransferEntry(TransferEntry.Kind.MAIL, key, new byte[0], id));
        var mailRequest = take();
        assertEquals(id, ((C2SPushMailPacket) mailRequest.packet()).uid());
        run(() -> complete(mailRequest, new A2AFlagResponse(true)));
        assertEquals(TransferResult.Status.SUCCESS, mail.get().status());
    }

    private RequestMsg<?> take() throws Exception {
        var request = requests.poll(5, TimeUnit.SECONDS);
        assertNotNull(request);
        return request;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void complete(RequestMsg<?> request, Object response) {
        ((dev.by1337.sync.common.callback.ResponseFuture) request.consumer()).complete(response);
    }

    private static void run(Runnable work) throws Exception {
        var result = new CompletableFuture<Void>();
        WORKER.executeReliably(() -> {
            try {
                work.run();
                result.complete(null);
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        result.get(5, TimeUnit.SECONDS);
    }
}
