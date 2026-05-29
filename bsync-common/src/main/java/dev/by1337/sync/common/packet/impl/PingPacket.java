package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record PingPacket() implements Packet {
    public PingPacket(ByteBuf buf, int protocolVersion) {
        this();
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
    }

}
