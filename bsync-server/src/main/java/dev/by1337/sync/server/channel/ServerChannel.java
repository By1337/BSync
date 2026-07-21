package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.messages.ClientConnectMessage;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import dev.by1337.sync.server.network.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

public class ServerChannel {
    private final Logger log;
    private final String id;
    private final EventLoopWorker eventLoop;
    private final Pipeline pipeline;
    private final DedicatedServer server;
    private final List<SocketConnection> connections = new CopyOnWriteArrayList<>();
    private Function<ServerChannelRuntime, ChannelRuntime> runtimeSpoofer;

    public ServerChannel(String id, EventLoopWorker eventLoop, DedicatedServer server) {
        this.id = id;
        this.eventLoop = eventLoop;
        log = LoggerFactory.getLogger(id + "|Channel");
        pipeline = new Pipeline(eventLoop);
        this.server = server;
    }

    public Pipeline pipeline() {
        return pipeline;
    }

    public void handle(ChannelMessage packet, Connection connection) {
        if (packet instanceof ClientConnectMessage(SocketConnection conn)) {
            connections.add(conn);
        } else if (packet instanceof ClientDisconnectMessage(SocketConnection conn)) {
            connections.remove(conn);
        }
        pipeline.execute(packet, lookup(connection));
    }

    public void forEachConnections(Consumer<dev.by1337.sync.common.channel.pipeline.Connection> c) {
        for (SocketConnection connection : connections) {
            c.accept(lookup(connection));
        }
    }

    private dev.by1337.sync.common.channel.pipeline.Connection lookup(SocketConnection connection) {
        return new dev.by1337.sync.common.channel.pipeline.Connection() {
            @Override
            public void write(ChannelMessage msg) {
                if (msg instanceof Packet packet)
                    connection.write(new ChanneledPacket(id, packet));
                else
                    throw new IllegalArgumentException(this + " only for packets! " + msg);
            }

            @Override
            public SocketConnection transport() {
                return connection;
            }

            @Override
            public String toString() {
                return connection.toString();
            }
        };
    }

    public void onRegister() {
        var self = this;
        Function<ServerChannelRuntime, ChannelRuntime> spoof = runtimeSpoofer != null ? runtimeSpoofer : v -> v;
        pipeline.registerAll(spoof.apply(new ServerChannelRuntime() {
            @Override
            public dev.by1337.sync.common.channel.pipeline.Connection lookup(SocketConnection connection) {
                return self.lookup(connection);
            }

            @Override
            public DedicatedServer server() {
                return self.server;
            }

            @Override
            public ServerChannel channel() {
                return self;
            }

            @Override
            public String name() {
                return self.id;
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
        }));
    }

    public void setRuntimeSpoofer(Function<ServerChannelRuntime, ChannelRuntime> runtimeSpoofer) {
        this.runtimeSpoofer = runtimeSpoofer;
    }

    public void close() {
        pipeline.closeAll();
    }

    public String id() {
        return id;
    }
}
