package dev.by1337.sync.k2v.login;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationPacketGateTest {
    @Test
    void holdsFinishAndFollowingPacketsUntilDataLoadsWithoutBlockingEventLoop() {
        var loaded = new CompletableFuture<Boolean>();
        var calls = new AtomicInteger();
        var gate = new ConfigurationPacketGate("finish"::equals, () -> {
            calls.incrementAndGet();
            return loaded;
        });
        var channel = new EmbeddedChannel(gate);
        try {
            assertTrue(channel.writeInbound("keepalive"));
            assertEquals("keepalive", channel.readInbound());
            assertFalse(channel.writeInbound("finish", "next"));
            assertNull(channel.readInbound());
            assertEquals(1, calls.get());
            var tick = new AtomicInteger();
            channel.eventLoop().execute(tick::incrementAndGet);
            channel.runPendingTasks();
            assertEquals(1, tick.get());
            loaded.complete(true);
            channel.runPendingTasks();
            assertEquals("finish", channel.readInbound());
            assertEquals("next", channel.readInbound());
            assertNull(channel.pipeline().context(gate));
            assertTrue(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void failedLoadClosesConnectionAndNeverForwardsFinish() {
        var loaded = new CompletableFuture<Boolean>();
        var channel = new EmbeddedChannel(new ConfigurationPacketGate(packet -> true, () -> loaded));
        var packet = Unpooled.buffer().writeByte(1);
        channel.writeInbound(packet);
        loaded.complete(false);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        assertNull(channel.readInbound());
        assertEquals(0, packet.refCnt());
        channel.finishAndReleaseAll();
    }

    @Test
    void disconnectReleasesHeldPacketsAndLateSuccessCannotResumeLogin() {
        var loaded = new CompletableFuture<Boolean>();
        var channel = new EmbeddedChannel(new ConfigurationPacketGate(packet -> true, () -> loaded));
        var packet = Unpooled.buffer().writeByte(1);
        channel.writeInbound(packet);
        channel.close();
        loaded.complete(true);
        channel.runPendingTasks();
        assertEquals(0, packet.refCnt());
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void removingPendingGateClosesConnectionInsteadOfAllowingUnloadedJoin() {
        var loaded = new CompletableFuture<Boolean>();
        var gate = new ConfigurationPacketGate(packet -> true, () -> loaded);
        var channel = new EmbeddedChannel(gate);
        channel.writeInbound("finish");
        channel.pipeline().remove(gate);
        loaded.complete(true);
        channel.runPendingTasks();
        assertFalse(channel.isActive());
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void loaderExceptionClosesConnection() {
        var channel = new EmbeddedChannel(new ConfigurationPacketGate(packet -> true, () -> {
            throw new IllegalStateException("scheduler closed");
        }));
        channel.writeInbound("finish");
        assertFalse(channel.isActive());
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void boundedQueueClosesConnectionOnPacketFlood() {
        var loaded = new CompletableFuture<Boolean>();
        var channel = new EmbeddedChannel(new ConfigurationPacketGate(packet -> true, () -> loaded));
        for (int i = 0; i < 257; i++) channel.writeInbound("packet");
        assertFalse(channel.isActive());
        loaded.complete(true);
        channel.runPendingTasks();
        assertNull(channel.readInbound());
        channel.finishAndReleaseAll();
    }
}
