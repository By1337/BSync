package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public final class S2CChannelStatsPacket implements Packet {
    public String id;
    public boolean opened;

    public S2CChannelStatsPacket(String id, boolean opened) {
        this.id = id;
        this.opened = opened;
    }

    public S2CChannelStatsPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        id = ByteBufCodecs.readUtf8(buf);
        opened = buf.readBoolean();
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUtf8(buf, id);
        buf.writeBoolean(opened);
    }

    @Override
    public int getId() {
        return Packets.S2C_CHANNEL_STATUS_PACKET;
    }

    @Override
    public String toString() {
        return "S2CChannelStatsPacket{" +
                "id='" + id + '\'' +
                ", opened=" + opened +
                '}';
    }
}
