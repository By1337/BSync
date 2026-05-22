package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public final class C2SUnlockPacket implements Packet {
    public final UUID key;

    public C2SUnlockPacket(UUID key) {
        this.key = key;
    }

    public C2SUnlockPacket(ByteBuf buf, int protocolVersion) {
        key = ByteBufCodecs.readUUID(buf);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
    }

    @Override
    public int getId() {
        return Packets.C2S_UNLOCK_PACKET;
    }

    @Override
    public String toString() {
        return "C2SUnlockPacket{" +
                "key=" + key +
                '}';
    }
}
