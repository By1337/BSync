package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;

public interface Packet {

    void read(ByteBuf buf, int protocolVersion);

    void write(ByteBuf buf, int protocolVersion);

    int getId();

    default Packet readAndGet(ByteBuf buf, int protocolVersion) {
        read(buf, protocolVersion);
        return this;
    }
}
