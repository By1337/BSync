package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientChannel implements dev.by1337.sync.common.channel.pipeline.Connection {
    private final Logger log;
    private final Connection connection;
    private final String id;
    private final EventLoopWorker eventLoop;
    private final Pipeline pipeline;
    private final ChannelType channelType;

    public ClientChannel(Connection connection, String id, EventLoopWorker eventLoop, ChannelType channelType) {
        this.connection = connection;
        this.id = id;
        log = LoggerFactory.getLogger(id + "|Channel");
        this.eventLoop = eventLoop;
        pipeline = new Pipeline(eventLoop);
        this.channelType = channelType;
        pipeline.addLast("requests", new RequestsHandler());
    }

    public Pipeline pipeline() {
        return pipeline;
    }

    public void handle(Packet packet) {
        pipeline.handle(packet, this);
    }

    @Override
    public void write(Packet msg) {
        connection.send(new ChanneledPacket(id, msg));
    }

    public void onRegister() {
        var self = this;
        pipeline.registerAll(new ClientChannelRuntime() {
            @Override
            public dev.by1337.sync.common.channel.pipeline.Connection freedom() {
                return self;
            }

            @Override
            public Pipeline pipeline() {
                return self.pipeline;
            }

            @Override
            public EventLoopWorker eventLoop() {
                return self.eventLoop;
            }

            @Override
            public Logger logger() {
                return self.log;
            }
        });
    }

    public void onChannelActive() {
        pipeline.handle(ChannelActiveMessage.INSTANCE, this);
    }

    public void onChannelInactive() {
        pipeline.handle(ChannelInactiveMessage.INSTANCE, this);
    }

    public void close() {
        pipeline.closeAll();
    }

    public String id() {
        return id;
    }

    public ChannelType getChannelType() {
        return channelType;
    }

}
