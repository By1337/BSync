package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record S2CPostLoginPacket() implements Packet {

    public S2CPostLoginPacket(ByteBuf buf, int protocolVersion) {
        this();
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
    }

    @Override
    public int getId() {
        return Packets.C2S_POST_LOGIN_PACKET;
    }
}
