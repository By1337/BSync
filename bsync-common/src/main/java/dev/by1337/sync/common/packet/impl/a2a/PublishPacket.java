package dev.by1337.sync.common.packet.impl.a2a;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

public record PublishPacket(byte[] payload) implements Packet {
    public PublishPacket(ByteBuf buf, int protocol) {
        this(ByteBufCodecs.readByteArray(buf));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeByteArray(buf, payload);
    }
}
