package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public record C2SUnlockAndFlushBlobPacket(UUID key, byte @Nullable [] blob) implements Packet {

    public C2SUnlockAndFlushBlobPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), ByteBufCodecs.readOptional(buf, ByteBufCodecs::readByteArray));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeOptional(buf, blob, ByteBufCodecs::writeByteArray);
    }

    @Override
    public int getId() {
        return Packets.C2S_UNLOCK_AND_FLUSH_BLOB_PACKET;
    }
}
