package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class PongPacket implements Packet {
    public long timestamp;
    public PongPacket(long timestamp) {
        this.timestamp = timestamp;
    }
    public PongPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        this.timestamp = buf.readLong();
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeLong(timestamp);
    }

    @Override
    public int getId() {
        return Packets.PONG_PACKET;
    }

    @Override
    public String toString() {
        return "PongPacket{" +
                "timestamp=" + timestamp +
                '}';
    }
}
