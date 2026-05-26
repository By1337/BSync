package dev.by1337.sync.common.netty.handler;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PacketEncoder extends MessageToByteEncoder<Packet> {
    private final int protocolVersion;

    public PacketEncoder(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet packet, ByteBuf byteBuf) throws Exception {
         System.out.println(ctx.channel().remoteAddress() + " SEND " + packet);
        Packets.write(byteBuf, protocolVersion, packet);
    }
}
