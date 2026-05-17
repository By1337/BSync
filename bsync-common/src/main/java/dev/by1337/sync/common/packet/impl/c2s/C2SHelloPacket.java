package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class C2SHelloPacket implements Packet {
    public int protocol;
    public String id;

    public C2SHelloPacket(int protocol, String id) {
        this.protocol = protocol;
        this.id = id;
    }

    public C2SHelloPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        protocol = buf.readInt();
        id = ByteBufCodecs.readUtf8(buf);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(protocolVersion);
        ByteBufCodecs.writeUtf8(buf, id);
    }

    @Override
    public int getId() {
        return Packets.C2S_HELLO_PACKET;
    }

    @Override
    public String toString() {
        return "C2SHelloPacket{" +
                "protocol=" + protocol +
                ", id='" + id + '\'' +
                '}';
    }
}
