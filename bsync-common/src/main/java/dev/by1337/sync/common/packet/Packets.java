package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public class Packets {
    public static final int PROTOCOL_VERSION = 3;
    public static final int LAST_SUPPORTED_VERSION = 3;

    public static final PacketRegistries REGISTRIES = new PacketRegistries();

    static {
        REGISTRIES.add("bsync:main", new PacketRegistry()
                .add(C2SHelloPacket.class, C2SHelloPacket::new)
                .add(S2CNoncePacket.class, S2CNoncePacket::new)
                .add(C2SLoginPacket.class, C2SLoginPacket::new)
                .add(S2CPostLoginPacket.class, S2CPostLoginPacket::new)
                .add(ChanneledPacket.class, ChanneledPacket::new)
                .add(RequestPacket.class, RequestPacket::new)
                .add(ResponsePacket.class, ResponsePacket::new)
                .add(PingPacket.class, PingPacket::new)
                .add(PongPacket.class, PongPacket::new)
                .add(AckRequest.class, AckRequest::new)
                .add(AckRequest.AckResponse.class, (v, v1) -> AckRequest.AckResponse.INSTANCE)
                .add(C2SOpenChannelPacket.class, C2SOpenChannelPacket::read)
                .add(C2SCloseChannelPacket.class, C2SCloseChannelPacket::new)
                .add(S2CChannelStatsPacket.class, S2CChannelStatsPacket::new)
        ).add("bsync:locks", new PacketRegistry()
                .add(S2CMailAcceptPacket.class, S2CMailAcceptPacket::new)
                .add(C2SMailResponsePacket.class, C2SMailResponsePacket::new)
                .add(C2SPushMailPacket.class, C2SPushMailPacket::new)
                .add(C2SPollAllMailsPacket.class, C2SPollAllMailsPacket::new)
                .add(S2CForceUnlockPacket.class, S2CForceUnlockPacket::new)
                .add(C2SLockAndGetBlobRequestPacket.class, C2SLockAndGetBlobRequestPacket::new)
                .add(S2CLockStatusAndBlobPacket.class, S2CLockStatusAndBlobPacket::new)
                .add(C2SUnlockAndFlushBlobPacket.class, C2SUnlockAndFlushBlobPacket::new)
                .add(C2SUnlockPacket.class, C2SUnlockPacket::new)
                .add(C2SRenewLockPacket.class, C2SRenewLockPacket::new)
                .add(C2SFlushBlobPacket.class, C2SFlushBlobPacket::new)
                .add(S2CFlushResponsePacket.class, S2CFlushResponsePacket::new)
        )
        ;
    }

    public static Packet read(ByteBuf buf, int protocolVersion) throws DecoderException {
        int registryId = buf.readUnsignedByte();
        var registry = REGISTRIES.getRegistry(registryId);
        int packetId = buf.readShort();
        return registry.getFactory(packetId).create(buf, protocolVersion);
    }

    public static void write(ByteBuf buf, int protocolVersion, Packet packet) {
        Class<? extends Packet> type = packet.getClass();
        var registry = REGISTRIES.getRegistry(type);

        buf.writeByte(REGISTRIES.getRegistryId(registry));
        buf.writeShort(registry.getId(type));
        packet.write(buf, protocolVersion);
    }

    public static boolean isSupportedProtocol(int version) {
        return version >= LAST_SUPPORTED_VERSION;
    }

    public static boolean isLegacyProtocol(int version) {
        return version < PROTOCOL_VERSION;
    }
}
