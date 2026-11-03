package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.c2s.C2SMailResponsePacket;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public record S2CMailAcceptPacket(UUID key, String json, int token, UUID uid) implements Packet, ExpectsResponse<C2SMailResponsePacket> {

    public S2CMailAcceptPacket(UUID key, String json, int token, long uid) {
        this(key, json, token, new UUID(0, uid));
    }

    public S2CMailAcceptPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), ByteBufCodecs.readUtf8(buf, dev.by1337.sync.common.mail.Mail.MAX_PAYLOAD_BYTES), buf.readInt(), ByteBufCodecs.readUUID(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeUtf8(buf, json);
        buf.writeInt(token);
        ByteBufCodecs.writeUUID(buf, uid);
    }

}
