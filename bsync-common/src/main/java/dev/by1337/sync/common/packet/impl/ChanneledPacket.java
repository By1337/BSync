package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record ChanneledPacket(String id, Packet payload) implements Packet {

    public ChanneledPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUtf8(buf), Packets.read(buf, protocolVersion));
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
}
