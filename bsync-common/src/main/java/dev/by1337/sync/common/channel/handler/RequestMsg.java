package dev.by1337.sync.common.channel.handler;

import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;

public record RequestMsg<T extends Packet>(Packet packet, PacketCallback<T> consumer, long timeoutMs) implements ChannelMessage {
}
