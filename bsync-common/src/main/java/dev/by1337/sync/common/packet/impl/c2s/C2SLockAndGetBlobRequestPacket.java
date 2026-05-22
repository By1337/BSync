package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusAndBlobPacket;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record C2SLockAndGetBlobRequestPacket(UUID key) implements Packet, ExpectsResponse<S2CLockStatusAndBlobPacket> {

    public C2SLockAndGetBlobRequestPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
    }

    @Override
    public int getId() {
        return Packets.C2S_LOCK_AND_GET_BLOB_PACKET;
    }
}
