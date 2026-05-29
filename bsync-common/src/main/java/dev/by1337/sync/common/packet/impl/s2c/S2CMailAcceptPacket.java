package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.c2s.C2SMailResponsePacket;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record S2CMailAcceptPacket(UUID key, String json, int token) implements Packet, ExpectsResponse<C2SMailResponsePacket> {

    public S2CMailAcceptPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), ByteBufCodecs.readUtf8(buf), buf.readInt());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeUtf8(buf, json);
        buf.writeInt(token);
    }

}
