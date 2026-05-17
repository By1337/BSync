package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public final class S2CNoncePacket implements Packet {
    public byte[] nonce;

    public S2CNoncePacket(byte[] nonce) {
        this.nonce = nonce;
    }

    public S2CNoncePacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        var size = buf.readInt();
        if (size > 256) throw new DecoderException("nonce size too large " + size + " bytes");
        nonce = new byte[size];
        buf.readBytes(nonce);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(nonce.length);
        buf.writeBytes(nonce);
    }

    @Override
    public int getId() {
        return Packets.S2C_NONCE_PACKET;
    }
}
