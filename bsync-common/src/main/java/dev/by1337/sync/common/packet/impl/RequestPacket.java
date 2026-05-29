package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record RequestPacket(int uid, Packet payload) implements Packet {

    public RequestPacket(ByteBuf buf, int protocolVersion) {
        this(buf.readInt(), Packets.read(buf, protocolVersion));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(uid);
        Packets.write(buf, protocolVersion, payload);
    }

}
