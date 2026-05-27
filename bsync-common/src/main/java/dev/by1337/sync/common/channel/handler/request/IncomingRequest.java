package dev.by1337.sync.common.channel.handler.request;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.ExpectsResponse;

import java.util.function.Consumer;

public class IncomingRequest implements ChannelMessage {
    private final Packet msg;
    private final Consumer<Packet> out;
    public int counter = 0;

    public IncomingRequest(Packet msg, Consumer<Packet> out) {
        this.msg = msg;
        this.out = out;
    }

    public ChannelMessage payload() {
        return msg;
    }

    public <T extends Packet> void response(ExpectsResponse<T> type, T response){
        out.accept(response);
    }
}
