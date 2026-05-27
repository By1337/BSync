package dev.by1337.sync.common.packet;


import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class PacketsTest {

    @Test
    public void readWriteTest() {
        assertReadWrite(new C2SCloseChannelPacket("test id\0"));
        assertReadWrite(new C2SHelloPacket(775, "test \0id"));
        assertReadWrite(new C2SLockAndGetBlobRequestPacket(UUID.randomUUID(), 775, true));
        assertReadWrite(new C2SLoginPacket(new byte[]{13, 37}));
        assertReadWrite(new C2SMailResponsePacket(C2SMailResponsePacket.Status.ACCEPTED, 775));
        assertReadWrite(new C2SOpenChannelPacket("id", ChannelType.LOCKS));
        assertReadWrite(new C2SPollAllMailsPacket(UUID.randomUUID(), 775));
        assertReadWrite(new C2SPushMailPacket(UUID.randomUUID(), "json"));
        assertReadWrite(new C2SRenewLockPacket(UUID.randomUUID(), 775));
        assertReadWrite(new C2SUnlockAndFlushBlobPacket(UUID.randomUUID(), new byte[]{13, 37}, 775));
        assertReadWrite(new C2SUnlockPacket(UUID.randomUUID(), 775));
        assertReadWrite(new S2CChannelStatsPacket("id", true));
        assertReadWrite(new S2CForceUnlockPacket(UUID.randomUUID(), 775));
        assertReadWrite(new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.ACCEPTED, new byte[]{13, 37}, 775, 665));
        assertReadWrite(new S2CMailAcceptPacket(UUID.randomUUID(), "json", 775));
        assertReadWrite(new S2CNoncePacket(new byte[]{13, 37}));
        assertReadWrite(new S2CPostLoginPacket());
        assertReadWrite(new ChanneledPacket("id", new S2CPostLoginPacket()));
        assertReadWrite(new PingPacket());
        assertReadWrite(new PongPacket(System.currentTimeMillis()));
        assertReadWrite(new RequestPacket(775, new S2CPostLoginPacket()));
        assertReadWrite(new ResponsePacket(775, new S2CPostLoginPacket()));
        assertReadWrite(new C2SFlushBlobPacket(UUID.randomUUID(), 775, 1337, new byte[]{13, 37}));
        assertReadWrite(new S2CFlushResponsePacket(true));
    }

    private <T extends Packet> void assertReadWrite(T packet) {
        ByteBuf buf = Unpooled.buffer();
        Packets.write(buf, Packets.PROTOCOL_VERSION, packet);
        Assert.assertEquals(Packets.read(buf, Packets.PROTOCOL_VERSION), packet);
        Assert.assertEquals(buf.readableBytes(), 0);
        buf.release();
    }
}