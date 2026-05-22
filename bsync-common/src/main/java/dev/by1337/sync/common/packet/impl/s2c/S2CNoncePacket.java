package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

import java.util.Arrays;
import java.util.Objects;

public record S2CNoncePacket(byte[] nonce) implements Packet {

    public S2CNoncePacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readByteArray(buf));
        if (nonce.length > 256) throw new DecoderException("nonce size too large " + nonce.length + " bytes");
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeByteArray(buf, nonce);
    }

    @Override
    public int getId() {
        return Packets.S2C_NONCE_PACKET;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        S2CNoncePacket that = (S2CNoncePacket) o;
        return Objects.deepEquals(nonce, that.nonce);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(nonce);
    }
}
