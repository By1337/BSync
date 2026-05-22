package dev.by1337.sync.server.channel.messages;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;

public record ClientConnectMessage(SocketConnection connection) implements ChannelMessage.UnhandledIgnored {
}
