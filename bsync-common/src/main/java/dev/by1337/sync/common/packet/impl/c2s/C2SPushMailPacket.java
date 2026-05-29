package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record C2SPushMailPacket(UUID key, String json) implements Packet {

    public C2SPushMailPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), ByteBufCodecs.readUtf8(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeUtf8(buf, json);
    }

}
