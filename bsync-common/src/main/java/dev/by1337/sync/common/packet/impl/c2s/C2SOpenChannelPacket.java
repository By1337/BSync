package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record C2SOpenChannelPacket(String id, String channelType) implements Packet {

    public static C2SOpenChannelPacket read(ByteBuf buf, int protocolVersion) {
        String id = ByteBufCodecs.readUtf8(buf);
        String channelType = ByteBufCodecs.readUtf8(buf);
        return new C2SOpenChannelPacket(id, channelType);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
        ByteBufCodecs.writeUtf8(buf, channelType);
    }

}
