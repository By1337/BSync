package dev.by1337.sync.server.channel.handler.pub;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelContext;
import dev.by1337.sync.common.channel.pipeline.ChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.packet.impl.a2a.PublishPacket;
import dev.by1337.sync.server.channel.ServerChannel;
import dev.by1337.sync.server.channel.ServerChannelRuntime;

public class PublisherHandler implements ChannelHandler {

    private ServerChannel channel;

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        channel = runtime.channel();
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof PublishPacket p) {
            channel.forEachConnections(conn -> {
                if (conn.transport() == ctx.connection().transport()) return;
                conn.write(p);
            });
        } else {
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
    }
}
