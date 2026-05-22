package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

import java.util.Arrays;

public final class C2SLoginPacket implements Packet {
    public final byte[] payload;

    public C2SLoginPacket(byte[] payload) {
        this.payload = payload;
    }

    public C2SLoginPacket(ByteBuf buf, int protocolVersion) {
        var size = buf.readInt();
        if (size > 256) throw new DecoderException("payload size too large " + size + " bytes");
        payload = new byte[size];
        buf.readBytes(payload);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(payload.length);
        buf.writeBytes(payload);
    }

    @Override
    public int getId() {
        return Packets.C2S_LOGIN_PACKET;
    }

    @Override
    public String toString() {
        return "C2SLoginPacket{" +
                "payload=" + Arrays.toString(payload) +
                '}';
    }
}
