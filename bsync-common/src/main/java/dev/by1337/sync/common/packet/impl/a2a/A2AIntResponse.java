package dev.by1337.sync.common.packet.impl.a2a;

import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record A2AIntResponse(int value) implements Packet {
    public A2AIntResponse(ByteBuf buf, int protocolVersion) {
        this(buf.readInt());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(value);
    }
}
