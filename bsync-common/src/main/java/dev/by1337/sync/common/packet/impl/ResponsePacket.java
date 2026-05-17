package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

public final class ResponsePacket implements Packet {
    public int uid;
    public @Nullable Packet payload;

    public ResponsePacket(int uid, @Nullable Packet payload) {
        this.uid = uid;
        this.payload = payload;
    }

    public ResponsePacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        uid = buf.readInt();
        if (buf.readBoolean()) {
            payload = Packets.read(buf, protocolVersion);
        }
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(uid);
        buf.writeBoolean(payload != null);
        if (payload != null) {
            Packets.write(buf, protocolVersion, payload);
        }
    }

    @Override
    public int getId() {
        return Packets.RESPONSE_PACKET;
    }

    @Override
    public String toString() {
        return "ResponsePacket{" +
                "uid=" + uid +
                ", payload=" + payload +
                '}';
    }
}
