package dev.by1337.sync.common.channel.handler;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;

import java.util.function.Consumer;

public record IncomingRequest(ChannelMessage msg, Consumer<Packet> out) implements ChannelMessage{
}
