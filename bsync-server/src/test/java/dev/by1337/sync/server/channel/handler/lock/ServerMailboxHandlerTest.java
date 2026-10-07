package dev.by1337.sync.server.channel.handler.lock;

import com.github.benmanes.caffeine.cache.Cache;
import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.handler.lock.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.IncomingRequest;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.packet.impl.a2a.A2ALongResponse;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.RequestPacket;
import dev.by1337.sync.common.packet.impl.ResponsePacket;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.database.table.BatchedMailbox;
import dev.by1337.sync.server.database.table.MailboxRepository;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ServerMailboxHandlerTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("mailbox-test");
    private final UUID player = new UUID(13, 37);
    private final List<RequestPacket> delivered = new ArrayList<>();
    private HikariDataSource dataSource;
    private MailboxRepository repository;
    private BatchedMailbox batched;
    private ServerMailboxHandler handler;
    private ServerLockerHandler locks;
    private Pipeline pipeline;
    private Pipeline senderPipeline;
    private Object mailBox;
    private final Connection connection = new Connection() {
        private final SocketConnection transport = this::write;

        @Override
        public void write(ChannelMessage msg) {
            delivered.add((RequestPacket) msg);
        }

        @Override
        public SocketConnection transport() {
            return transport;
        }
    };

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL");
        dataSource.setMaximumPoolSize(1);
        repository = new MailboxRepository(dataSource, "test_mailbox");
        run(() -> {
            batched = new BatchedMailbox(repository, WORKER);
            var type = Class.forName(ServerMailboxHandler.class.getName() + "$MailBox");
            var constructor = type.getDeclaredConstructor(BatchedMailbox.class);
            constructor.setAccessible(true);
            mailBox = constructor.newInstance(batched);
            handler = new ServerMailboxHandler();
            locks = new ServerLockerHandler();
            pipeline = new Pipeline(WORKER).addLast("mailbox", handler);
            // Wire the handler to a real H2 repository without starting a TCP server.
            set(handler, "mailBox", mailBox);
            set(handler, "locks", locks);
            set(handler, "pipeline", pipeline);
            set(handler, "eventLoop", WORKER);
            set(handler, "serverChannel", Proxy.newProxyInstance(
                    ServerChannelRuntime.class.getClassLoader(),
                    new Class<?>[]{ServerChannelRuntime.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("lookup")) return connection;
                        throw new UnsupportedOperationException(method.getName());
                    }));
            pipeline.getHandler("$requests").init(new ChannelRuntime() {
                public Pipeline pipeline() { return pipeline; }
                public EventLoopWorker eventLoop() { return WORKER; }
                public Logger logger() { return LoggerFactory.getLogger(ServerMailboxHandlerTest.class); }
            });
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (senderPipeline != null) senderPipeline.closeAll().get(5, TimeUnit.SECONDS);
            if (pipeline != null) pipeline.closeAll().get(5, TimeUnit.SECONDS);
        } finally {
            if (dataSource != null) dataSource.close();
        }
    }

    @Test
    void allocatedRangesAndServerLocalPushesNeverOverlap() throws Exception {
        run(() -> {
            var offsets = new ArrayList<Long>();
            pipeline.execute(new IncomingRequest(new C2SGiveMailUidsRequest(),
                    response -> offsets.add(((A2ALongResponse) response).value())), connection);
            handler.pushMail(player, "local");
            var another = new ServerMailboxHandler();
            new Pipeline(WORKER).addLast("mailbox", another).execute(
                    new IncomingRequest(new C2SGiveMailUidsRequest(),
                            response -> offsets.add(((A2ALongResponse) response).value())), connection);
            assertEquals(C2SGiveMailUidsRequest.RANGE_SIZE + 1L, offsets.get(1) - offsets.get(0));
            flush();
            assertEquals(List.of(new MailboxRepository.Mail(1, player, "local")),
                    repository.loadAll(player));
        });
    }

    @Test
    void lostPushConfirmationRetriesSameIdAndDuplicateReturnsTrue() throws Exception {
        var acknowledged = new CompletableFuture<Void>();
        var sends = new ArrayList<C2SPushMailPacket>();
        var confirmations = new ArrayList<A2AFlagResponse>();
        run(() -> {
            senderPipeline = new Pipeline(WORKER);
            var sender = new ClientLocksHandler(Locks.Type.ONLY_MAILBOX);
            sender.lockManager(new LockManager() {
                public boolean ensureLockOwnership(UUID key) { return true; }
                public void acceptMail(UUID key, String json) { fail("Unexpected local delivery"); }
                public void forceUnlock(UUID key) {}
                public void close() {}
            });
            senderPipeline.addLast("locks", sender);
            var backendConnection = new Connection() {
                private final SocketConnection transport = msg -> {};
                public SocketConnection transport() { return transport; }
                public void write(ChannelMessage msg) {
                    var response = (ResponsePacket) msg;
                    if (response.payload() instanceof A2AFlagResponse flag) {
                        confirmations.add(flag);
                        if (confirmations.size() == 1) return; // Lose only the first confirmation.
                        senderPipeline.execute(msg, senderPipeline.local());
                        acknowledged.complete(null);
                    } else {
                        senderPipeline.execute(msg, senderPipeline.local());
                    }
                }
            };
            var remote = new Connection() {
                public SocketConnection transport() { return backendConnection.transport(); }
                public void write(ChannelMessage msg) {
                    if (msg instanceof RequestPacket request
                            && request.payload() instanceof C2SPushMailPacket push) sends.add(push);
                    pipeline.execute(msg, backendConnection);
                }
            };
            senderPipeline.registerAll(new ClientChannelRuntime() {
                public Connection remote() { return remote; }
                public Pipeline pipeline() { return senderPipeline; }
                public EventLoopWorker eventLoop() { return WORKER; }
                public Logger logger() { return LoggerFactory.getLogger(ServerMailboxHandlerTest.class); }
            });
            senderPipeline.execute(ChannelActiveMessage.INSTANCE, remote);
            sender.pushMail(player, "mail");
        });
        acknowledged.get(12, TimeUnit.SECONDS);
        run(() -> {
            flush();
            assertEquals(2, sends.size());
            assertEquals(sends.getFirst(), sends.getLast());
            assertEquals(List.of(new A2AFlagResponse(true), new A2AFlagResponse(true)), confirmations);
            assertEquals(List.of(new MailboxRepository.Mail(1, player, "mail")),
                    repository.loadAll(player));
            assertEquals(1, queue().size());
        });
    }

    @Test
    void retransmittedPacketIsStoredOnceAndDoesNotConsumeMailIds() throws Exception {
        var packet = new C2SPushMailPacket(player, "first", Long.MIN_VALUE);
        run(() -> {
            push(packet);
            push(roundTrip(packet, Packets.PROTOCOL_VERSION));
            push(new C2SPushMailPacket(player, "second", Long.MAX_VALUE));
            flush();
            assertEquals(List.of(
                    new MailboxRepository.Mail(1, player, "first"),
                    new MailboxRepository.Mail(2, player, "second")), repository.loadAll(player));
            assertEquals(2, queue().size());
        });
    }

    @Test
    void identicalPayloadsWithDifferentIdsRemainSeparateMails() throws Exception {
        run(() -> {
            push(new C2SPushMailPacket(player, "same", 0));
            push(new C2SPushMailPacket(player, "same", 1));
            flush();
            assertEquals(2, repository.loadAll(player).size());
            assertEquals(2, queue().size());
        });
    }

    @Test
    void rotationKeepsPreviousWindowAndExpiresIdsAfterTwoRotations() throws Exception {
        var first = new C2SPushMailPacket(player, "first", 10);
        var second = new C2SPushMailPacket(player, "second", 11);
        run(() -> {
            push(first);
            invoke(handler, "rotateDiffs");
            push(first);
            push(second);
            invoke(handler, "rotateDiffs");
            push(first);
            push(second);
            flush();
            assertEquals(List.of("first", "second", "first"),
                    repository.loadAll(player).stream().map(MailboxRepository.Mail::payload).toList());
            assertEquals(3, queue().size());
        });
    }

    @Test
    void acceptedMailIsDeletedFromDatabaseAndCacheAndRetryCannotResurrectIt() throws Exception {
        var packet = new C2SPushMailPacket(player, "mail", 42);
        run(() -> {
            push(packet);
            flush();
            acquireLock();
            poll();
            respond(C2SMailResponsePacket.accepted(token()));
            flush();
            assertTrue(repository.loadAll(player).isEmpty());
            assertTrue(queue().isEmpty());
            invalidateCache();
            assertTrue(queue().isEmpty());
            push(roundTrip(packet, Packets.PROTOCOL_VERSION));
            poll();
            flush();
            assertEquals(1, delivered.size());
            assertTrue(queue().isEmpty());
            assertTrue(repository.loadAll(player).isEmpty());
        });
    }

    @Test
    void acceptingBeforeInsertFlushLeavesNoMailInDatabase() throws Exception {
        run(() -> {
            acquireLock();
            push(new C2SPushMailPacket(player, "mail", 42));
            respond(C2SMailResponsePacket.accepted(token()));
            flush();
            assertTrue(repository.loadAll(player).isEmpty());
            invalidateCache();
            assertTrue(queue().isEmpty());
        });
    }

    @Test
    void acceptingNextMailPreservesOrderAndDeletesOnlyAcceptedMail() throws Exception {
        run(() -> {
            push(new C2SPushMailPacket(player, "first", 1));
            push(new C2SPushMailPacket(player, "second", 2));
            acquireLock();
            poll();
            respond(C2SMailResponsePacket.accepted(token()));
            flush();
            assertEquals(List.of(new MailboxRepository.Mail(2, player, "second")),
                    repository.loadAll(player));
            assertEquals(2, delivered.size());
            assertEquals("second", ((S2CMailAcceptPacket) delivered.getLast().payload()).json());
            respond(C2SMailResponsePacket.accepted(token()));
            flush();
            invalidateCache();
            assertTrue(queue().isEmpty());
            assertTrue(repository.loadAll(player).isEmpty());
        });
    }

    @Test
    void retriesDuringDeliveryDoNotCreateAnotherRequestOrStoredMail() throws Exception {
        var packet = new C2SPushMailPacket(player, "mail", 42);
        run(() -> {
            acquireLock();
            push(packet);
            push(roundTrip(packet, Packets.PROTOCOL_VERSION));
            poll();
            flush();
            assertEquals(1, delivered.size());
            assertEquals(1, queue().size());
            assertEquals(1, repository.loadAll(player).size());
            respond(C2SMailResponsePacket.accepted(token()));
            push(packet);
            flush();
            assertEquals(1, delivered.size());
            assertTrue(queue().isEmpty());
            assertTrue(repository.loadAll(player).isEmpty());
        });
    }

    @Test
    void closingBackendFlushesAcceptedDeletionAndPreservesOtherOwners() throws Exception {
        var other = new UUID(7, 8);
        run(() -> {
            repository.put(1, other, "other");
            acquireLock();
            push(new C2SPushMailPacket(player, "mail", 42));
            respond(C2SMailResponsePacket.accepted(token()));
            handler.close();
            var reloaded = new MailboxRepository(dataSource, "test_mailbox");
            assertTrue(reloaded.loadAll(player).isEmpty());
            assertEquals(List.of(new MailboxRepository.Mail(1, other, "other")),
                    reloaded.loadAll(other));
        });
    }

    @Test
    void rejectedMailRemainsAvailableForAnotherPoll() throws Exception {
        run(() -> {
            acquireLock();
            push(new C2SPushMailPacket(player, "mail", 42));
            respond(C2SMailResponsePacket.reject(token()));
            flush();
            assertEquals(1, repository.loadAll(player).size());
            assertEquals(1, queue().size());
            poll();
            assertEquals(2, delivered.size());
        });
    }

    @Test
    void acceptanceAfterUnlockDeletesMailInsteadOfRedeliveringOnNextLogin() throws Exception {
        run(() -> {
            acquireLock();
            int originalToken = token();
            push(new C2SPushMailPacket(player, "mail", 42));
            ((ServerLockerHandler.LockMap) get(locks, "lockMap")).unlock(player);
            respond(C2SMailResponsePacket.accepted(originalToken));
            flush();
            invalidateCache();
            acquireLock();
            poll();
            assertEquals(1, delivered.size());
            assertTrue(queue().isEmpty());
            assertTrue(repository.loadAll(player).isEmpty());
        });
    }

    @Test
    void acceptanceAfterRelockStillDeletesMailForOriginalDelivery() throws Exception {
        run(() -> {
            acquireLock();
            int originalToken = token();
            push(new C2SPushMailPacket(player, "mail", 42));
            ((ServerLockerHandler.LockMap) get(locks, "lockMap")).unlock(player);
            acquireLock();
            assertNotEquals(originalToken, token());
            respond(C2SMailResponsePacket.accepted(originalToken));
            flush();
            invalidateCache();
            poll();
            assertEquals(1, delivered.size());
            assertTrue(queue().isEmpty());
            assertTrue(repository.loadAll(player).isEmpty());
        });
    }

    @Test
    void acceptanceWithWrongLockTokenDoesNotDeleteMail() throws Exception {
        run(() -> {
            acquireLock();
            push(new C2SPushMailPacket(player, "mail", 42));
            respond(C2SMailResponsePacket.accepted(token() + 1));
            flush();
            assertEquals(1, repository.loadAll(player).size());
            assertEquals(1, queue().size());
        });
    }

    @Test
    void packetCodecPreservesLongIdAndOldProtocolsAreRejected() {
        var packet = new C2SPushMailPacket(player, "line one\nстрока два", Long.MIN_VALUE);
        assertEquals(packet, roundTrip(packet, Packets.PROTOCOL_VERSION));
        assertFalse(Packets.isSupportedProtocol(7));
        assertTrue(Packets.isSupportedProtocol(Packets.PROTOCOL_VERSION));
    }

    private void push(C2SPushMailPacket packet) {
        var responses = new ArrayList<A2AFlagResponse>();
        pipeline.execute(new IncomingRequest(packet,
                response -> responses.add((A2AFlagResponse) response)), connection);
        assertEquals(List.of(new A2AFlagResponse(true)), responses);
    }

    private void acquireLock() throws Exception {
        ((ServerLockerHandler.LockMap) get(locks, "lockMap")).tryLock(player, connection.transport());
    }

    private int token() {
        return locks.getLock(player).token;
    }

    private void poll() {
        pipeline.execute(new C2SPollAllMailsPacket(player, token()), connection);
    }

    private void respond(C2SMailResponsePacket response) {
        pipeline.execute(new ResponsePacket(delivered.getLast().uid(), response), connection);
    }

    @SuppressWarnings("unchecked")
    private Queue<MailboxRepository.Mail> queue() throws Exception {
        var method = mailBox.getClass().getDeclaredMethod("getMailQueue", UUID.class);
        method.setAccessible(true);
        return (Queue<MailboxRepository.Mail>) method.invoke(mailBox, player);
    }

    private void invalidateCache() throws Exception {
        ((Cache<?, ?>) get(mailBox, "loaded_mails")).invalidateAll();
    }

    private void flush() throws Exception {
        invoke(get(batched, "addBatcher"), "ioTick");
        invoke(get(batched, "removeBatcher"), "ioTick");
    }

    private static C2SPushMailPacket roundTrip(C2SPushMailPacket packet, int version) {
        var buf = Unpooled.buffer();
        try {
            packet.write(buf, version);
            var decoded = new C2SPushMailPacket(buf, version);
            assertFalse(buf.isReadable());
            return decoded;
        } finally {
            buf.release();
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

    private static void invoke(Object target, String name) throws Exception {
        var method = target.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        method.invoke(target);
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
