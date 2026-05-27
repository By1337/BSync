package dev.by1337.sync.common.netty.handler;

import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PacketDecoder extends ByteToMessageDecoder {
    private static final boolean LOG_PACKETS = Boolean.getBoolean("bsync.packet_log");
    private static final Logger log = LoggerFactory.getLogger(PacketDecoder.class);
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
        if (LOG_PACKETS){
            log.info("[READ:{}] {}", ctx.channel().remoteAddress(), v);
        }
        out.add(v);
    }
}
