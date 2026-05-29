package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record S2CChannelStatsPacket(String id, boolean opened) implements Packet {

    public S2CChannelStatsPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUtf8(buf), buf.readBoolean());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
        buf.writeBoolean(opened);
    }

}
