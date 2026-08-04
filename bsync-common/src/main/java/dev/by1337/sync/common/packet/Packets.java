package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.a2a.PublishPacket;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Packets {
    public static final int PROTOCOL_VERSION = 5;
    public static final int LAST_SUPPORTED_VERSION = 5;

    public static final PacketRegistry BSYNC_MAIN = new PacketRegistry("bsync:main", PROTOCOL_VERSION)
            .add(0, C2SHelloPacket.class, C2SHelloPacket::new)
            .add(1, S2CNoncePacket.class, S2CNoncePacket::new)
            .add(2, C2SLoginPacket.class, C2SLoginPacket::new)
            .add(3, S2CPostLoginPacket.class, S2CPostLoginPacket::new)
            .add(4, ChanneledPacket.class, ChanneledPacket::read)
            .add(5, RequestPacket.class, RequestPacket::new)
            .add(6, ResponsePacket.class, ResponsePacket::new)
            .add(7, PingPacket.class, PingPacket::new)
            .add(8, PongPacket.class, PongPacket::new)
            .add(9, AckRequest.class, AckRequest::new)
            .add(10, AckRequest.AckResponse.class, (v, v1) -> AckRequest.AckResponse.INSTANCE)
            .add(11, C2SOpenChannelPacket.class, C2SOpenChannelPacket::read)
            .add(12, C2SCloseChannelPacket.class, C2SCloseChannelPacket::new)
            .add(13, S2CChannelStatsPacket.class, S2CChannelStatsPacket::new)
            .lock();
    public static final PacketRegistry BSYNC_LOCKS = new PacketRegistry("bsync:locks", PROTOCOL_VERSION)
            .add(0, S2CMailAcceptPacket.class, S2CMailAcceptPacket::new)
            .add(1, C2SMailResponsePacket.class, C2SMailResponsePacket::new)
            .add(2, C2SPushMailPacket.class, C2SPushMailPacket::new)
            .add(3, C2SPollAllMailsPacket.class, C2SPollAllMailsPacket::new)
            .add(4, S2CForceUnlockPacket.class, S2CForceUnlockPacket::new)
            .add(5, C2SLockAndGetBlobRequestPacket.class, C2SLockAndGetBlobRequestPacket::new)
            .add(6, S2CLockStatusAndBlobPacket.class, S2CLockStatusAndBlobPacket::new)
            .add(7, C2SUnlockAndFlushBlobPacket.class, C2SUnlockAndFlushBlobPacket::new)
            .add(8, C2SUnlockPacket.class, C2SUnlockPacket::new)
            .add(9, C2SRenewLockPacket.class, C2SRenewLockPacket::new)
            .add(10, C2SFlushBlobPacket.class, C2SFlushBlobPacket::new)
            .add(11, S2CFlushResponsePacket.class, S2CFlushResponsePacket::new)
            .lock();
    public static final PacketRegistry BSYNC_LOGS = new PacketRegistry("bsync:logs", PROTOCOL_VERSION)
            .add(0, C2SWriteLogPacket.class, C2SWriteLogPacket::read)
            .lock();
    public static final PacketRegistry BSYNC_PUBLISH = new PacketRegistry("bsync:publish", PROTOCOL_VERSION)
            .add(0, PublishPacket.class, PublishPacket::new)
            .lock();

    public static Packet readGlobal(ByteBuf buf, int protocolVersion) throws DecoderException {
        var registries = ChannelRegistryContext.getCurrentChannel();
        if (registries != null) {
            return read(buf, registries);
        }
        int registryId = buf.readUnsignedByte();
        if (registryId != 0) {
            throw new DecoderException("Invalid registry id " + registryId);
        }
        int packetId = buf.readShort();
        return BSYNC_MAIN.getFactory(packetId).create(buf, protocolVersion);
    }

    public static void writeGlobal(ByteBuf buf, int protocolVersion, Packet packet) {
        var registries = ChannelRegistryContext.getCurrentChannel();
        if (registries != null) {
            write(buf, registries, packet);
            return;
        }
        Class<? extends Packet> type = packet.getClass();
        buf.writeByte(0);
        buf.writeShort(BSYNC_MAIN.getId(type));
        packet.write(buf, protocolVersion);
    }

    public static Packet read(ByteBuf buf, PacketRegistries registries) throws DecoderException {
        int registryId = buf.readUnsignedByte();
        var registry = registries.getRegistry(registryId);
        int packetId = buf.readShort();
        return registry.getFactory(packetId).create(buf, registries.getVersion(registryId));
    }

    public static void write(ByteBuf buf, PacketRegistries registries, Packet packet) {
        Class<? extends Packet> type = packet.getClass();
        var registry = registries.getRegistry(type);
        int registryId = registries.getRegistryId(registry);
        buf.writeByte(registryId);
        buf.writeShort(registry.getId(type));
        packet.write(buf, registries.getVersion(registryId));
    }


    public static boolean isSupportedProtocol(int version) {
        return version >= LAST_SUPPORTED_VERSION;
    }

    public static boolean isLegacyProtocol(int version) {
        return version < PROTOCOL_VERSION;
    }
}
