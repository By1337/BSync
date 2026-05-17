package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public final class S2CMailAcceptPacket implements Packet {
    public UUID key;
    public String json;

    public S2CMailAcceptPacket() {
    }

    public S2CMailAcceptPacket(UUID key, String json) {
        this.key = key;
        this.json = json;
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        key = ByteBufCodecs.readUUID(buf);
        json = ByteBufCodecs.readUtf8(buf);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeUtf8(buf, json);
    }

    @Override
    public int getId() {
        return Packets.S2C_MAIL_ACCEPT_PACKET;
    }

    @Override
    public String toString() {
        return "S2CMailAcceptPacket{" +
                "key=" + key +
                ", json='" + json + '\'' +
                '}';
    }
}
