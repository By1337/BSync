package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

import java.util.Arrays;
import java.util.Objects;

public record C2SLoginPacket(byte[] payload) implements Packet {

    public C2SLoginPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readByteArray(buf));
        if (payload.length > 256) throw new DecoderException("payload size too large " + payload.length + " bytes");
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeByteArray(buf, payload);
    }

    @Override
    public int getId() {
        return Packets.C2S_LOGIN_PACKET;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        C2SLoginPacket that = (C2SLoginPacket) o;
        return Objects.deepEquals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(payload);
    }
}
