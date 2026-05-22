package dev.by1337.sync.common.netty.handler;

import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.util.List;

public class PacketDecoder extends ByteToMessageDecoder {
    private final int protocolVersion;

    public PacketDecoder(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
        var v = Packets.read(buf, protocolVersion);
        if (buf.readableBytes() > 0) {
            throw new DecoderException("Packet " + v + " has more bytes than expected " + buf.readableBytes());
        }
        System.out.println("READ " + v);
        out.add(v);
    }
}
