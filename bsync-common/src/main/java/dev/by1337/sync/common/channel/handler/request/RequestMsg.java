package dev.by1337.sync.common.channel.handler.request;

import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.ChannelMessage;

public record RequestMsg<T extends ChannelMessage>(ChannelMessage packet, ResponseFuture<T> consumer,
                                                   long timeoutMs) implements ChannelMessage {
}
