package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record S2CFlushResponsePacket(boolean accepted) implements Packet {

    public S2CFlushResponsePacket(ByteBuf buf, int protocolVersion) {
        this(buf.readBoolean());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeBoolean(accepted);
    }

    @Override
    public int getId() {
        return Packets.S2C_FLUSH_ACCEPT_RESPONSE;
    }
}
