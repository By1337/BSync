package dev.by1337.sync.common.packet;


import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

public class PacketsTest {

    @Test
    public void readWriteTest() {
        PacketRegistries registries = new PacketRegistries();
        registries.add(0, Packets.BSYNC_MAIN.id(), Packets.BSYNC_MAIN);
        registries.add(1, Packets.BSYNC_LOCKS.id(), Packets.BSYNC_LOCKS);
        registries.add(2, Packets.BSYNC_LOGS.id(), Packets.BSYNC_LOGS);
        registries.add(3, Packets.BSYNC_PUBLISH.id(), Packets.BSYNC_PUBLISH);
        assertReadWrite(registries, new C2SCloseChannelPacket("test id\0"));
        assertReadWrite(registries, new C2SHelloPacket(775, "test \0id"));
        assertReadWrite(registries, new C2SLockAndGetBlobRequestPacket(UUID.randomUUID(), 775, true));
        assertReadWrite(registries, new C2SLoginPacket(new byte[]{13, 37}));
        assertReadWrite(registries, new C2SMailResponsePacket(C2SMailResponsePacket.Status.ACCEPTED, 775));
        assertReadWrite(registries, new C2SOpenChannelPacket("id", ChannelType.LOCKS, new PacketRegistries.Snapshot(List.of())));
        assertReadWrite(registries, new C2SPollAllMailsPacket(UUID.randomUUID(), 775));
        assertReadWrite(registries, new C2SPushMailPacket(UUID.randomUUID(), "json"));
        assertReadWrite(registries, new C2SRenewLockPacket(UUID.randomUUID(), 775));
        assertReadWrite(registries, new C2SUnlockAndFlushBlobPacket(UUID.randomUUID(), new byte[]{13, 37}, 775));
        assertReadWrite(registries, new C2SUnlockPacket(UUID.randomUUID(), 775));
        assertReadWrite(registries, new S2CChannelStatsPacket("id", true));
        assertReadWrite(registries, new S2CForceUnlockPacket(UUID.randomUUID(), 775));
        assertReadWrite(registries, new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.ACCEPTED, new byte[]{13, 37}, 775, 665));
        assertReadWrite(registries, new S2CMailAcceptPacket(UUID.randomUUID(), "json", 775));
        assertReadWrite(registries, new S2CNoncePacket(new byte[]{13, 37}));
        assertReadWrite(registries, new S2CPostLoginPacket());
        //ChannelRegistryContext.onChannelOpen("id", registries);
        //assertReadWrite(registries, new ChanneledPacket("id", new S2CPostLoginPacket()));
        //ChannelRegistryContext.onChannelClose("id");
        assertReadWrite(registries, new PingPacket());
        assertReadWrite(registries, new PongPacket(System.currentTimeMillis()));
        assertReadWrite(registries, new RequestPacket(775, new S2CPostLoginPacket()));
        assertReadWrite(registries, new ResponsePacket(775, new S2CPostLoginPacket()));
        assertReadWrite(registries, new C2SFlushBlobPacket(UUID.randomUUID(), 775, 1337, new byte[]{13, 37}));
        assertReadWrite(registries, new S2CFlushResponsePacket(true));
    }

    private <T extends Packet> void assertReadWrite(PacketRegistries registries, T packet) {
        ByteBuf buf = Unpooled.buffer();
        Packets.write(buf, registries, packet);
        Assert.assertEquals(Packets.read(buf, registries), packet);
        Assert.assertEquals(buf.readableBytes(), 0);
        buf.release();
    }
}