package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.network.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerChannel {
    private final Logger log;
    private final String id;
    private final EventLoopWorker eventLoop;
    private final Pipeline pipeline;

    public ServerChannel(String id, EventLoopWorker eventLoop) {
        this.id = id;
        this.eventLoop = eventLoop;
        log = LoggerFactory.getLogger(id + "|Channel");
        pipeline = new Pipeline(eventLoop);
        pipeline.addLast("requests", new RequestsHandler());
    }

    public Pipeline pipeline() {
        return pipeline;
    }

    public void handle(ChannelMessage packet, Connection connection) {
        pipeline.handle(packet, lookup(connection));
    }

    private dev.by1337.sync.common.channel.pipeline.Connection lookup(SocketConnection connection) {
        return new dev.by1337.sync.common.channel.pipeline.Connection() {
            @Override
            public void write(Packet msg) {
                connection.write(new ChanneledPacket(id, msg));
            }

            @Override
            public SocketConnection transport() {
                return connection;
            }
        };
    }

    public void onRegister() {
        var self = this;
        pipeline.registerAll(new ServerChannelRuntime() {
            @Override
            public dev.by1337.sync.common.channel.pipeline.Connection lookup(SocketConnection connection) {
                return self.lookup(connection);
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
        }, () -> {});
    }
    public void close() {
        pipeline.closeAll();
    }
    public String id() {
        return id;
    }
}
