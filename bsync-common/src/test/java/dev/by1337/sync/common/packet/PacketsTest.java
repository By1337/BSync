package dev.by1337.sync.common.packet;


import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import dev.by1337.sync.common.packet.impl.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

public class PacketsTest {


    @Test
    public void readWriteTes12t() {
        int shardCount = 2;
        int[] counters = new int[shardCount];

        for (int i = 0; i < 10; i++) {
            int x = (int) (hash(UUID.randomUUID()) % shardCount);
            if (x < 0) x = -x;
            counters[x]++;
        }

        for (int i = 0; i < shardCount; i++) {
            System.out.println("Shard " + i + ": " + counters[i]);
        }
    }

    static long hash(UUID uuid) {
       return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }
    @Test
    public void readWriteTest(){
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
        assertReadWrite(new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.ACCEPTED, new byte[]{13,37}, 775, 665));
        assertReadWrite(new S2CMailAcceptPacket(UUID.randomUUID(), "json", 775));
        assertReadWrite(new S2CNoncePacket(new byte[]{13, 37}));
        assertReadWrite(new S2CPostLoginPacket(UUID.randomUUID()));
        assertReadWrite(new ChanneledPacket("id", new S2CPostLoginPacket(UUID.randomUUID())));
        assertReadWrite(new PingPacket());
        assertReadWrite(new PongPacket(System.currentTimeMillis()));
        assertReadWrite(new RequestPacket(775, new S2CPostLoginPacket(UUID.randomUUID())));
        assertReadWrite(new ResponsePacket(775, new S2CPostLoginPacket(UUID.randomUUID())));
        assertReadWrite(new C2SFlushBlobPacket(UUID.randomUUID(), 775, 1337, new byte[]{13,37}));
        assertReadWrite(new S2CFlushResponsePacket(true));
    }

    private <T extends Packet> void  assertReadWrite(T packet){
        ByteBuf buf = Unpooled.buffer();
        Packets.write(buf, Packets.PROTOCOL_VERSION, packet);
        Assert.assertEquals(Packets.read(buf, Packets.PROTOCOL_VERSION), packet);
        Assert.assertEquals(buf.readableBytes(), 0);
        buf.release();
    }
}