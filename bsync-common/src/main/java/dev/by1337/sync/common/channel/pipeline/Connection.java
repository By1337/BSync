package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;

public interface Connection {
    void write(ChannelMessage msg);
    SocketConnection transport();
}
