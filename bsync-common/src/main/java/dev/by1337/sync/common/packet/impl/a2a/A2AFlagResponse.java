package dev.by1337.sync.common.packet.impl.a2a;

import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record A2AFlagResponse(boolean flag) implements Packet {
    public A2AFlagResponse(ByteBuf buf, int protocolVersion) {
        this(buf.readBoolean());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeBoolean(flag);
    }
}
