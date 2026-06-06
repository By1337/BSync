package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.server.DedicatedServer;

public interface ServerChannelRuntime extends ChannelRuntime {
    Connection lookup(SocketConnection connection);
    DedicatedServer server();
    ServerChannel channel();
    String name();
}
