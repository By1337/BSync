package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.packet.Packet;

import java.util.function.Consumer;

public interface BaseServerChannelRuntime extends ChannelRuntime {
    void forEachConnections(Consumer<Connection> c);

    default void broadcast(Packet packet) {
        forEachConnections(c -> c.write(packet));
    }
    String name();
}
