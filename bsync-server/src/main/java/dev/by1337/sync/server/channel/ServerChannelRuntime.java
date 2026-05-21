package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;

public interface ServerChannelRuntime extends ChannelRuntime {
    Connection lookup(SocketConnection connection);
}
