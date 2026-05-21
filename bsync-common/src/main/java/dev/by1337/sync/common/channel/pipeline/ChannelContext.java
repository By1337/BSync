package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.channel.ChannelMessage;

public interface ChannelContext {
    void fire(ChannelMessage msg);

    Pipeline pipeline();

    Connection connection();
}
