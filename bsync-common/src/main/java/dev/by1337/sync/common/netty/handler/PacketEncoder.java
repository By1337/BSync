package dev.by1337.sync.common.netty.handler;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacketEncoder extends MessageToByteEncoder<Packet> {
    private static final boolean LOG_PACKETS = Boolean.getBoolean("bsync.packet_log");
    private static final Logger log = LoggerFactory.getLogger(PacketEncoder.class);
    private final int protocolVersion;

    public PacketEncoder(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet packet, ByteBuf byteBuf) throws Exception {
        if (LOG_PACKETS) {
            log.info("[SEND:{}] {}", ctx.channel().remoteAddress(), packet);
        }
        Packets.write(byteBuf, protocolVersion, packet);
    }
}
