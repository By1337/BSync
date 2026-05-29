package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record S2CPostLoginPacket() implements Packet {

    public S2CPostLoginPacket(ByteBuf buf, int protocolVersion) {
        this();
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
    }

}
