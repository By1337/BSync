package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.pipeline.BaseServerChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.server.DedicatedServer;

import java.util.function.Consumer;

public interface ServerChannelRuntime extends BaseServerChannelRuntime {
    Connection lookup(SocketConnection connection);

    DedicatedServer server();

    ServerChannel channel();

    String name();

    @Override
    default void forEachConnections(Consumer<Connection> c) {
        channel().forEachConnections(c);
    }
}
