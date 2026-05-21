package dev.by1337.sync.server.channel.messages;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;

public record ClientDisconnectMessage(SocketConnection connection) implements ChannelMessage {
}
