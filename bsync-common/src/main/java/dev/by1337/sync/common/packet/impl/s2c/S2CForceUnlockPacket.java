package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record S2CForceUnlockPacket(UUID key, int token) implements Packet {

    public S2CForceUnlockPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), buf.readInt());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        buf.writeInt(token);
    }

}
