package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record C2SCloseChannelPacket(String id) implements Packet {

    public C2SCloseChannelPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUtf8(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
    }

    @Override
    public int getId() {
        return Packets.C2S_CLOSE_CHANNEL_PACKET;
    }
}
