package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public final class S2CForceUnlockPacket implements Packet {
    public UUID key;

    public S2CForceUnlockPacket(UUID key) {
        this.key = key;
    }

    public S2CForceUnlockPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        key = ByteBufCodecs.readUUID(buf);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
    }

    @Override
    public int getId() {
        return Packets.S2C_FORCE_UNLOCK_PACKET;
    }

}
