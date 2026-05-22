package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record PongPacket(long timestamp) implements Packet {

    public PongPacket(ByteBuf buf, int protocolVersion) {
        this(buf.readLong());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeLong(timestamp);
    }

    @Override
    public int getId() {
        return Packets.PONG_PACKET;
    }
}
