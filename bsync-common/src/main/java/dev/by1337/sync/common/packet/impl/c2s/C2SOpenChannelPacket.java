package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public final class C2SOpenChannelPacket implements Packet {
    public String id;
    public ChannelType channelType;

    public C2SOpenChannelPacket(String id, ChannelType channelType) {
        this.id = id;
        this.channelType = channelType;
    }

    public C2SOpenChannelPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        id= ByteBufCodecs.readUtf8(buf);
        channelType = ChannelType.fromId(buf.readByte());
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

    @Override
    public String toString() {
        return "C2SOpenChannelPacket{" +
                "id='" + id + '\'' +
                ", channelType=" + channelType +
                '}';
    }
}
