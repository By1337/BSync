package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.client.channel.handler.lock.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.common.channel.handler.request.IncomingRequest;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.c2s.C2SMailResponsePacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ClientMailboxHandlerTest {
    private final UUID player = new UUID(13, 37);
    private final List<String> processed = new ArrayList<>();
    private final List<C2SMailResponsePacket> responses = new ArrayList<>();
    private ClientLocksHandler handler;
    private static final int TOKEN = 17;

    @BeforeEach
    void setUp() throws Exception {
        handler = new ClientLocksHandler(Locks.Type.ONLY_MAILBOX);
        handler.lockManager(new LockManager() {
            public boolean ensureLockOwnership(UUID key) { return true; }
            public void acceptMail(UUID key, String json) { processed.add(json); }
            public void forceUnlock(UUID key) { }
            public void close() { }
        });
        setLock(TOKEN);
    }

    @Test
    void repeatedDeliveryIsAcknowledgedEveryTimeButProcessedOnce() {
        var mail = new S2CMailAcceptPacket(player, "mail", TOKEN, Long.MIN_VALUE);
        deliver(mail);
        deliver(roundTrip(mail, Packets.PROTOCOL_VERSION));
        assertEquals(List.of("mail"), processed);
        assertEquals(List.of(C2SMailResponsePacket.accepted(TOKEN),
                C2SMailResponsePacket.accepted(TOKEN)), responses);
    }

    @Test
    void differentIdsWithIdenticalPayloadAreBothProcessed() {
        deliver(new S2CMailAcceptPacket(player, "mail", TOKEN, 0));
        deliver(new S2CMailAcceptPacket(player, "mail", TOKEN, Long.MAX_VALUE));
        assertEquals(List.of("mail", "mail"), processed);
    }

    @Test
    void redeliveryAfterRelockIsAcknowledgedWithNewTokenWithoutReprocessing() throws Exception {
        deliver(new S2CMailAcceptPacket(player, "mail", TOKEN, 42));
        setLock(TOKEN + 1);
        deliver(new S2CMailAcceptPacket(player, "mail", TOKEN + 1, 42));
        assertEquals(List.of("mail"), processed);
        assertEquals(C2SMailResponsePacket.accepted(TOKEN + 1), responses.getLast());
    }

    @Test
    void rejectedOutdatedDeliveryDoesNotMarkIdAsProcessed() {
        deliver(new S2CMailAcceptPacket(player, "mail", TOKEN - 1, 42));
        assertTrue(processed.isEmpty());
        assertTrue(responses.getLast().isRejected());
        deliver(new S2CMailAcceptPacket(player, "mail", TOKEN, 42));
        assertEquals(List.of("mail"), processed);
        assertTrue(responses.getLast().isAccepted());
    }

    @Test
    void rotationPreservesPreviousWindowAndExpiresAfterTwoRotations() throws Exception {
        var first = new S2CMailAcceptPacket(player, "first", TOKEN, 1);
        var second = new S2CMailAcceptPacket(player, "second", TOKEN, 2);
        deliver(first);
        rotate();
        deliver(first);
        deliver(second);
        rotate();
        deliver(first);
        deliver(second);
        assertEquals(List.of("first", "second", "first"), processed);
        assertEquals(5, responses.size());
        assertTrue(responses.stream().allMatch(C2SMailResponsePacket::isAccepted));
    }

    @Test
    void packetCodecPreservesDeliveryId() {
        var mail = new S2CMailAcceptPacket(player, "mail", TOKEN, 42);
        assertEquals(mail, roundTrip(mail, Packets.PROTOCOL_VERSION));
    }

    private void deliver(S2CMailAcceptPacket mail) {
        handler.handle(null, new IncomingRequest(mail,
                response -> responses.add((C2SMailResponsePacket) response)));
    }

    private static S2CMailAcceptPacket roundTrip(S2CMailAcceptPacket mail, int version) {
        var buf = Unpooled.buffer();
        try {
            mail.write(buf, version);
            var decoded = new S2CMailAcceptPacket(buf, version);
            assertFalse(buf.isReadable());
            return decoded;
        } finally {
            buf.release();
        }
    }

    @SuppressWarnings("unchecked")
    private void setLock(int token) throws Exception {
        var type = Class.forName(ClientLocksHandler.class.getName() + "$LockData");
        var constructor = type.getDeclaredConstructor(int.class, UUID.class);
        constructor.setAccessible(true);
        var lock = constructor.newInstance(1, player);
        var tokenField = type.getDeclaredField("token");
        tokenField.setAccessible(true);
        tokenField.setInt(lock, token);
        var pendingField = type.getDeclaredField("pending");
        pendingField.setAccessible(true);
        pendingField.setBoolean(lock, false);
        var locksField = ClientLocksHandler.class.getDeclaredField("locks");
        locksField.setAccessible(true);
        ((Map<UUID, Object>) locksField.get(handler)).put(player, lock);
    }

    private void rotate() throws Exception {
        var method = ClientLocksHandler.class.getDeclaredMethod("rotateDiffs");
        method.setAccessible(true);
        method.invoke(handler);
    }
}
