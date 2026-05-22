package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record C2SOpenChannelPacket(String id, ChannelType channelType) implements Packet {

    public C2SOpenChannelPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUtf8(buf), ChannelType.fromId(buf.readByte()));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
        buf.writeByte(channelType.id);
    }

    @Override
    public int getId() {
        return Packets.C2S_OPEN_CHANNEL_PACKET;
    }
}
