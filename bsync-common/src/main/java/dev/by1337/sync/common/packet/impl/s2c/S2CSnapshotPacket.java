package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

public record S2CSnapshotPacket(byte @Nullable [] snapshot) implements Packet {
    public S2CSnapshotPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readOptional(buf, ByteBufCodecs::readByteArray));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeOptional(buf, snapshot, ByteBufCodecs::writeByteArray);
    }
}
