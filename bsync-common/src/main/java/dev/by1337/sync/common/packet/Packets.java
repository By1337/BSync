package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.packet.c2s.C2SHelloPacket;
import dev.by1337.sync.common.packet.c2s.C2SLoginPacket;
import dev.by1337.sync.common.packet.s2c.S2CNoncePacket;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public class Packets {
    public static final int PROTOCOL_VERSION = 1;

    public static final int C2S_HELLO_PACKET = 0;
    public static final int S2C_NONCE_PACKET = 1;
    public static final int C2S_LOGIN_PACKET = 2;


    public static Packet read(ByteBuf buf, int protocolVersion) throws DecoderException {
        if (buf.readableBytes() < 1) throw new DecoderException("Invalid packet length");
        int id = buf.readUnsignedByte();
        var res = switch (id) {
            case C2S_HELLO_PACKET -> new C2SHelloPacket().readAndGet(buf, protocolVersion);
            case S2C_NONCE_PACKET -> new S2CNoncePacket().readAndGet(buf, protocolVersion);
            case C2S_LOGIN_PACKET -> new C2SLoginPacket().readAndGet(buf, protocolVersion);
            default -> throw new DecoderException("Invalid packet id " + id);
        };
        if (buf.readableBytes() > 0) {
            throw new DecoderException("Packet " + id + " has more bytes than expected " + buf.readableBytes());
        }
        return res;
    }

    public static void write(ByteBuf buf, int protocolVersion, Packet packet) {
        buf.writeByte(packet.getId());
        packet.write(buf, protocolVersion);
    }
}
