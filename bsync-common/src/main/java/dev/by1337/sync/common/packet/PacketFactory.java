package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;

@FunctionalInterface
public interface PacketFactory<T extends Packet> {
    T create(ByteBuf buf, int protocol);
}
