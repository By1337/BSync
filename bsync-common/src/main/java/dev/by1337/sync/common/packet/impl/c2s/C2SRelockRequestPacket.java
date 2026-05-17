package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public final class C2SRelockRequestPacket implements Packet {
    public UUID key;

    public C2SRelockRequestPacket() {
    }

    public C2SRelockRequestPacket(UUID key) {
        this.key = key;
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
        return Packets.C2S_RELOCK_PACKET;
    }

    @Override
    public String toString() {
        return "C2SRelockRequestPacket{" +
                "key=" + key +
                '}';
    }
}
