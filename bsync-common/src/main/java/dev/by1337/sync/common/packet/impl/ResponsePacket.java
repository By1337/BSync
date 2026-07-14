package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.EncoderException;
import org.jetbrains.annotations.Nullable;

public record ResponsePacket(int uid, @Nullable ChannelMessage payload) implements Packet {

    public ResponsePacket(ByteBuf buf, int protocolVersion) {
        this(buf.readInt(), buf.readBoolean() ? Packets.read(buf, protocolVersion) : null);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(uid);
        buf.writeBoolean(payload != null);
        if (payload != null) {
            if (payload instanceof Packet p)
                Packets.write(buf, protocolVersion, p);
            else throw new EncoderException("Trying to send " + payload);
        }
    }

}
