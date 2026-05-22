package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusResponsePacket;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record C2SRelockRequestPacket(UUID key) implements Packet, ExpectsResponse<S2CLockStatusResponsePacket> {

    public C2SRelockRequestPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
    }

    @Override
    public int getId() {
        return Packets.C2S_RELOCK_PACKET;
    }
}
