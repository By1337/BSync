package dev.by1337.sync.client.channel.status;

import dev.by1337.sync.common.channel.ChannelMessage;

public record ChannelInactiveMessage() implements ChannelMessage.UnhandledIgnored {
    public static final ChannelMessage INSTANCE = new ChannelInactiveMessage();
}
