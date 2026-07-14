package dev.by1337.sync.client.channel;

import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;

public interface ClientChannelRuntime extends ChannelRuntime {
    Connection remote();
}
