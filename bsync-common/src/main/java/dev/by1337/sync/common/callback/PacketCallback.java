package dev.by1337.sync.common.callback;

import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.packet.Packet;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

@FunctionalInterface
public interface PacketCallback {
    void accept(@Nullable Packet packet, Connection connection);
}
