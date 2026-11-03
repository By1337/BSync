package dev.by1337.sync.server.channel.handler.lock;

import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.common.channel.*;
import dev.by1337.sync.common.channel.handler.request.IncomingRequest;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.mail.*;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.database.table.ReliableMailboxRepository;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;
import org.slf4j.*;

import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ServerMailboxHandlerTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("mailbox-test");
    private static final EventLoopWorker IO = new EventLoopWorker("mailbox-test-io");
    private final UUID player = UUID.randomUUID();
    private final BlockingQueue<RequestPacket> delivered = new LinkedBlockingQueue<>();
    private HikariDataSource source;
    private ReliableMailboxRepository repository;
    private ServerMailboxHandler handler;
    private ServerLockerHandler locks;
    private Pipeline pipeline;
    private final Connection connection = new Connection() {
        private final SocketConnection transport = this::write;

        public void write(ChannelMessage msg) {
            delivered.add((RequestPacket) msg);
        }

        public SocketConnection transport() {
            return transport;
        }
    };

    @BeforeEach
    void setUp() throws Exception {
        source = new HikariDataSource();
        source.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL");
        source.setMaximumPoolSize(1);
        repository = new ReliableMailboxRepository(source, "test_mailbox");
        run(WORKER, () -> {
            handler = new ServerMailboxHandler();
            locks = new ServerLockerHandler();
            pipeline = new Pipeline(WORKER).addLast("mailbox", handler);
            set(handler, "mailbox", repository);
            set(handler, "localOutbox", new MailJournal(null));
            set(handler, "locks", locks);
            set(handler, "pipeline", pipeline);
            set(handler, "eventLoop", WORKER);
            set(handler, "io", IO);
            set(handler, "serverChannel", Proxy.newProxyInstance(ServerChannelRuntime.class.getClassLoader(),
                    new Class<?>[]{ ServerChannelRuntime.class }, (proxy, method, args) -> {
                        if (method.getName().equals("lookup")) return connection;
                        throw new UnsupportedOperationException(method.getName());
                    }));
            pipeline.getHandler("$requests").init(new ChannelRuntime() {
                public Pipeline pipeline() {
                    return pipeline;
                }

                public EventLoopWorker eventLoop() {
                    return WORKER;
                }

                public Logger logger() {
                    return LoggerFactory.getLogger(ServerMailboxHandlerTest.class);
                }
            });
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        drain();
        pipeline.closeAll().get(5, TimeUnit.SECONDS);
        source.close();
    }

    @Test
    void pushAckMeansMailAlreadyCommittedAndDuplicatesRemainSingleAcrossRestart() throws Exception {
        Mail mail = new Mail(UUID.randomUUID(), player, "reward");
        assertTrue(push(mail));
        assertEquals(mail, repository.next(player).mail());
        set(handler, "mailbox", new ReliableMailboxRepository(source, "test_mailbox"));
        assertTrue(push(mail));
        repository.acknowledge(mail.id());
        assertTrue(push(mail));
        assertNull(repository.next(player));
    }

    @Test
    void acceptedDeliveryIsDurablyAcknowledgedAndNotResurrectedByProducerRetry() throws Exception {
        Mail mail = new Mail(UUID.randomUUID(), player, "reward");
        assertTrue(push(mail));
        acquire();
        poll();
        var delivery = delivered.poll(5, TimeUnit.SECONDS);
        assertNotNull(delivery);
        assertEquals(mail.id(), ((S2CMailAcceptPacket) delivery.payload()).uid());
        respond(delivery, C2SMailResponsePacket.accepted(token()));
        drain();
        assertNull(repository.next(player));
        assertTrue(push(mail));
        assertNull(repository.next(player));
        assertTrue(delivered.isEmpty());
    }

    @Test
    void failedHandlerKeepsMailAndRetryUsesSameId() throws Exception {
        Mail mail = new Mail(UUID.randomUUID(), player, "reward");
        assertTrue(push(mail));
        acquire();
        poll();
        var first = delivered.poll(5, TimeUnit.SECONDS);
        assertNotNull(first);
        respond(first, C2SMailResponsePacket.reject(token()));
        drain();
        assertEquals(mail, repository.next(player).mail());
        assertEquals(1, repository.next(player).attempts());
        // Advance persisted backoff without waiting wall clock time.
        repository.failed(mail.id(), "test retry", 0);
        poll();
        var retry = delivered.poll(5, TimeUnit.SECONDS);
        assertNotNull(retry);
        assertEquals(first.payload(), retry.payload());
        respond(retry, C2SMailResponsePacket.accepted(token()));
        drain();
        assertNull(repository.next(player));
    }

    @Test
    void wrongTokenCannotAcknowledgeOrDeleteMail() throws Exception {
        Mail mail = new Mail(UUID.randomUUID(), player, "reward");
        assertTrue(push(mail));
        acquire();
        poll();
        var delivery = delivered.poll(5, TimeUnit.SECONDS);
        assertNotNull(delivery);
        respond(delivery, C2SMailResponsePacket.accepted(token() + 1));
        drain();
        assertEquals(mail, repository.next(player).mail());
    }

    @Test
    void acceptanceAfterUnlockAppliesToOriginalDelivery() throws Exception {
        Mail mail = new Mail(UUID.randomUUID(), player, "reward");
        assertTrue(push(mail));
        acquire();
        poll();
        var delivery = delivered.poll(5, TimeUnit.SECONDS);
        assertNotNull(delivery);
        int originalToken = token();
        run(WORKER, () -> ((ServerLockerHandler.LockMap) get(locks, "lockMap")).unlock(player));
        respond(delivery, C2SMailResponsePacket.accepted(originalToken));
        drain();
        assertNull(repository.next(player));
    }

    @Test
    void conflictingIdNeverAcknowledgesDifferentMessageAsSuccess() throws Exception {
        UUID id = UUID.randomUUID();
        assertTrue(push(new Mail(id, player, "original")));
        assertFalse(push(new Mail(id, player, "different")));
        assertEquals("original", repository.next(player).mail().payload());
    }

    @Test
    void packetCodecPreservesUuidAndRejectsOldProtocol() {
        var packet = new C2SPushMailPacket(player, "x".repeat(65_536) + "\nстрока два", UUID.randomUUID());
        var buf = Unpooled.buffer();
        try {
            packet.write(buf, Packets.PROTOCOL_VERSION);
            assertEquals(packet, new C2SPushMailPacket(buf, Packets.PROTOCOL_VERSION));
            assertFalse(buf.isReadable());
            assertFalse(Packets.isSupportedProtocol(8));
        } finally {
            buf.release();
        }
    }

    @Test
    void mailboxExportHonorsTheClientSuppliedShardSelection() throws Exception {
        UUID other = new UUID(0, 1), matching = new UUID(0, 2);
        repository.put(new Mail(UUID.randomUUID(), other, "other shard"));
        Mail expected = new Mail(UUID.randomUUID(), matching, "matching shard");
        repository.put(expected);
        var result = new CompletableFuture<dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket>();
        run(WORKER, () -> handler.exportPage(new C2SExportPagePacket(
                dev.by1337.sync.common.transfer.TransferEntry.Kind.MAIL, null, 0, 2, 0)).then(result::complete));
        var page = result.get(5, TimeUnit.SECONDS);
        assertEquals(1, page.entries().size());
        assertEquals(expected.id(), page.entries().get(0).messageId());
        assertEquals(matching, page.entries().get(0).key());
        assertTrue(page.done());
    }

    private boolean push(Mail mail) throws Exception {
        var result = new CompletableFuture<Boolean>();
        run(WORKER, () -> pipeline.execute(new IncomingRequest(
                new C2SPushMailPacket(mail.owner(), mail.payload(), mail.id()),
                response -> result.complete(((A2AFlagResponse) response).flag())), connection));
        return result.get(5, TimeUnit.SECONDS);
    }

    private void acquire() throws Exception {
        run(WORKER, () -> ((ServerLockerHandler.LockMap) get(locks, "lockMap")).tryLock(player, connection.transport()));
    }

    private int token() {
        return locks.getLock(player).token;
    }

    private void poll() throws Exception {
        run(WORKER, () -> pipeline.execute(new C2SPollAllMailsPacket(player, token()), connection));
    }

    private void respond(RequestPacket request, C2SMailResponsePacket response) throws Exception {
        run(WORKER, () -> pipeline.execute(new ResponsePacket(request.uid(), response), connection));
    }

    private void drain() throws Exception {
        for (int i = 0; i < 4; i++) {
            run(IO, () -> {
            });
            run(WORKER, () -> {
            });
        }
    }

    private static Object get(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void set(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void run(EventLoopWorker worker, CheckedRunnable action) throws Exception {
        var result = new CompletableFuture<Void>();
        worker.execute(() -> {
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
