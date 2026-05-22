package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class PingPacket implements Packet {

    public PingPacket() {
    }

    public PingPacket(ByteBuf buf, int protocolVersion) {
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
    }

    @Override
    public int getId() {
        return Packets.PING_PACKET;
    }

    @Override
    public String toString() {
        return "PingPacket{}";
    }
}
