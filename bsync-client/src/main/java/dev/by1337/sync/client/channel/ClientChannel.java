package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientChannel implements dev.by1337.sync.common.channel.pipeline.Connection {
    private final Logger log;
    private final Connection connection;
    private final String id;
    private final EventLoopWorker eventLoop;
    private final Pipeline pipeline;
    private final ChannelType channelType;
    private volatile boolean registered;

    public ClientChannel(Connection connection, String id, EventLoopWorker eventLoop, ChannelType channelType) {
        this.connection = connection;
        this.id = id;
        log = LoggerFactory.getLogger(id + "|Channel");
        this.eventLoop = eventLoop;
        pipeline = new Pipeline(eventLoop);
        this.channelType = channelType;
    }

    public Pipeline pipeline() {
        return pipeline;
    }

    public void handle(Packet packet) {
        pipeline.execute(packet, this);
    }

    @Override
    public void write(Packet msg) {
        connection.write(new ChanneledPacket(id, msg));
    }

    @Override
    public SocketConnection transport() {
        return connection;
    }

    private final AtomicBoolean isRegistered = new AtomicBoolean();

    public void onRegister() {
        if (!isRegistered.compareAndSet(false, true)) return;
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
        }, () -> registered = true);
    }

    public boolean registered() {
        return registered;
    }

    private final AtomicBoolean channelActive = new AtomicBoolean();

    public void onChannelActive() {
        if (!channelActive.compareAndSet(false, true)) return;
        pipeline.execute(ChannelActiveMessage.INSTANCE, this);
    }

    public void onChannelInactive() {
        if (!channelActive.compareAndSet(true, false)) return;
        pipeline.execute(ChannelInactiveMessage.INSTANCE, this);
    }

    public CompletableFuture<Void> close() {
        return pipeline.closeAll();
    }

    public String id() {
        return id;
    }

    public ChannelType getChannelType() {
        return channelType;
    }

    public EventLoopWorker eventLoop() {
        return eventLoop;
    }
}
