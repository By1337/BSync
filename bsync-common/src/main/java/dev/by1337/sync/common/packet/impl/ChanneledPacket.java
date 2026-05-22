package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class ChanneledPacket implements Packet {
    public final String id;
    public final Packet payload;

    public ChanneledPacket(String id, Packet payload) {
        this.id = id;
        this.payload = payload;
    }

    public ChanneledPacket(ByteBuf buf, int protocolVersion) {
        id = ByteBufCodecs.readUtf8(buf);
        payload = Packets.read(buf, protocolVersion);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
        Packets.write(buf, protocolVersion, payload);
    }

    @Override
    public int getId() {
        return Packets.CHANNELED_PACKET;
    }

    @Override
    public String toString() {
        return "ChanneledPacket{" +
                "id='" + id + '\'' +
                ", payload=" + payload +
                '}';
    }
}
