package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record C2SOpenChannelPacket(String id, String channelType) implements Packet {

    public static C2SOpenChannelPacket read(ByteBuf buf, int protocolVersion) {
        String id;
        String channelType;
        id = ByteBufCodecs.readUtf8(buf);
        if (protocolVersion == 1) {
            channelType = buf.readByte() == 0 ? ChannelType.LOCKS : "default:custom";
        } else {
            channelType = ByteBufCodecs.readUtf8(buf);
        }
        return new C2SOpenChannelPacket(id, channelType);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
        if (protocolVersion == 1) {
            buf.writeByte(channelType.equals(ChannelType.LOCKS) ? 0 : 1);
        } else {
            ByteBufCodecs.writeUtf8(buf, channelType);
        }
    }

    @Override
    public int getId() {
        return Packets.C2S_OPEN_CHANNEL_PACKET;
    }
}
