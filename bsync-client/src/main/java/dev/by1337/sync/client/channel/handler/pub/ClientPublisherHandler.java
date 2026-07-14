package dev.by1337.sync.client.channel.handler.pub;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelContext;
import dev.by1337.sync.common.channel.pipeline.ChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.packet.impl.a2a.PublishPacket;

import java.util.function.Consumer;

public class ClientPublisherHandler implements ChannelHandler, Consumer<byte[]> {
    private Connection remote;
    private boolean closing;
    private final Consumer<byte[]> accept;

    public ClientPublisherHandler(Consumer<byte[]> accept) {
        this.accept = accept;
    }


    @Override
    public void init(ChannelRuntime runtime) {
        if (!(runtime instanceof ClientChannelRuntime ccr)) throw new IllegalArgumentException("Invalid runtime type");
        remote = ccr.remote();
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof PublishPacket p) {
            accept.accept(p.payload());
        } else {
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
        closing = true;
    }

    @Override
    public void accept(byte[] bytes) {
        remote.write(new PublishPacket(bytes));
    }
}
