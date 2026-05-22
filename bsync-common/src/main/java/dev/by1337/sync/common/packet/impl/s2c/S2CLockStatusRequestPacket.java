package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.c2s.C2SLockStatusResponsePacket;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public final class S2CLockStatusRequestPacket implements Packet, ExpectsResponse<C2SLockStatusResponsePacket> {
    public final UUID key;

    public S2CLockStatusRequestPacket(UUID key) {
        this.key = key;
    }

    public S2CLockStatusRequestPacket(ByteBuf buf, int protocolVersion) {
        key = ByteBufCodecs.readUUID(buf);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
    }

    @Override
    public int getId() {
        return Packets.S2C_LOCK_STATUS_REQUEST_PACKET;
    }

}
