package dev.by1337.sync.common.channel.handler;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;

import java.util.function.Consumer;

public class IncomingRequest implements ChannelMessage {
    private final ChannelMessage msg;
    private final Consumer<Packet> out;
    public int counter = 0;

    public IncomingRequest(ChannelMessage msg, Consumer<Packet> out) {
        this.msg = msg;
        this.out = out;
    }

    public ChannelMessage payload() {
        return msg;
    }

    public Consumer<Packet> out() {
        return out;
    }
}
