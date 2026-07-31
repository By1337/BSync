package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.EncoderException;

public record RequestPacket(int uid, ChannelMessage payload) implements Packet {

    public RequestPacket(ByteBuf buf, int protocolVersion) {
        this(buf.readInt(), Packets.readGlobal(buf, protocolVersion));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeInt(uid);
        if (payload instanceof Packet p)
            Packets.writeGlobal(buf, protocolVersion, p);
        else throw new EncoderException("Trying to send " + payload);
    }

}
