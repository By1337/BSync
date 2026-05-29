package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.channel.ChannelMessage;
import io.netty.buffer.ByteBuf;

public interface Packet extends ChannelMessage {

    void write(ByteBuf buf, int protocolVersion);

}
