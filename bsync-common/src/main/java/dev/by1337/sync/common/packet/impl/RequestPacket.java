package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class RequestPacket implements Packet {
    public int uid;
    public Packet payload;

    public RequestPacket(int uid, Packet payload) {
        this.uid = uid;
        this.payload = payload;
    }

    public RequestPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        uid = buf.readInt();
        payload = Packets.read(buf, protocolVersion);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(uid);
        Packets.write(buf, protocolVersion, payload);
    }

    @Override
    public int getId() {
        return Packets.REQUEST_PACKET;
    }

    @Override
    public String toString() {
        return "RequestPacket{" +
                "uid=" + uid +
                ", payload=" + payload +
                '}';
    }
}
