package dev.by1337.sync.common.callback;

import dev.by1337.sync.common.packet.Packet;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

@FunctionalInterface
public interface PacketCallback extends Consumer<@Nullable Packet> {

}
