package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record C2SHelloPacket(int protocol, String id) implements Packet {

    public C2SHelloPacket(ByteBuf buf, int protocolVersion) {
        this(buf.readInt(), ByteBufCodecs.readUtf8(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(protocol);
        ByteBufCodecs.writeUtf8(buf, id);
    }

}
