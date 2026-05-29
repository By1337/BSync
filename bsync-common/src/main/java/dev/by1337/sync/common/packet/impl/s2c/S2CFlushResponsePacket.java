package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record S2CFlushResponsePacket(boolean accepted) implements Packet {

    public S2CFlushResponsePacket(ByteBuf buf, int protocolVersion) {
        this(buf.readBoolean());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeBoolean(accepted);
    }

}
