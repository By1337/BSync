package dev.by1337.sync.common.packet.impl.a2a;

import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record A2ALongResponse(long value) implements Packet {
    public A2ALongResponse(ByteBuf buf, int protocolVersion) {
        this(buf.readLong());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeLong(value);
    }
}
