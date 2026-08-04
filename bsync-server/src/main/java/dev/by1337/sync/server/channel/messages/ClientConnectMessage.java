package dev.by1337.sync.server.channel.messages;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.PacketRegistries;

public record ClientConnectMessage(SocketConnection connection, PacketRegistries.Snapshot registries) implements ChannelMessage.UnhandledIgnored {
}
