package dev.by1337.sync.common.packet.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class S2CPostLoginPacket implements Packet {


    public S2CPostLoginPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
    }

    @Override
    public int getId() {
        return Packets.C2S_POST_LOGIN_PACKET;
    }
}
