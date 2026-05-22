package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

import java.util.UUID;

public final class C2SPushMailPacket implements Packet {
    public final UUID key;
    public final String json;

    public C2SPushMailPacket(UUID key, String json) {
        this.key = key;
        this.json = json;
    }

    public C2SPushMailPacket(ByteBuf buf, int protocolVersion) {
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
        return Packets.C2S_PUSH_MAIL_PACKET;
    }

    @Override
    public String toString() {
        return "C2SPushMailPacket{" +
                "key=" + key +
                ", json='" + json + '\'' +
                '}';
    }
}
