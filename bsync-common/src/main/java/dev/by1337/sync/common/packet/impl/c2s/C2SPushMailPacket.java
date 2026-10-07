package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record C2SPushMailPacket(UUID key, String json, long uid) implements Packet, ExpectsResponse<A2AFlagResponse> {

    public C2SPushMailPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), ByteBufCodecs.readUtf8(buf), buf.readLong());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeUtf8(buf, json);
        buf.writeLong(uid);
    }

}
