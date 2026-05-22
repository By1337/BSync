package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record C2SRenewLockPacket(UUID key, int token) implements Packet {

    public C2SRenewLockPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), buf.readInt());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        buf.writeInt(token);
    }

    @Override
    public int getId() {
        return Packets.C2S_RENEW_LOCK_PACKET;
    }
}
