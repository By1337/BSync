package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SCloseChannelPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SOpenChannelPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CChannelStatsPacket;
import dev.by1337.sync.common.work.EventLoopWorkers;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.handler.ServerLockHandler;
import dev.by1337.sync.server.channel.messages.ClientConnectMessage;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import dev.by1337.sync.server.network.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public class ChannelManager {
    private static final Logger log = LoggerFactory.getLogger(ChannelManager.class);
    private final EventLoopWorkers workers;
    private final Map<String, ServerChannel> channels = new ConcurrentHashMap<>();
    private final DedicatedServer server;
    private final LongAdder receivedPackets = new LongAdder();

    public ChannelManager(EventLoopWorkers workers, DedicatedServer server) {
        this.workers = workers;
        this.server = server;
    }

    public ServerChannel addChannel(String id, Consumer<ServerChannel> init) {
        if (channels.containsKey(id)) {
            throw new IllegalArgumentException("Channel with id " + id + " already exists");
        }
        ServerChannel serverChannel = new ServerChannel(
                id,
                workers.getNext(),
                server
        );
        init.accept(serverChannel);
        channels.put(id, serverChannel);
        serverChannel.onRegister();
        return serverChannel;
    }

    public void onReceive(Packet packet, Connection connection) {
        receivedPackets.increment();
        if (packet instanceof C2SCloseChannelPacket(String id1)) {
            var channel = channels.get(id1);
            if (channel != null) {
                channel.handle(new ClientDisconnectMessage(connection), connection);
            } else {
                log.error("Try to close unknown channel {} {}", id1, connection);
            }
        } else if (packet instanceof ChanneledPacket(String id, Packet payload)) {
            var channel = channels.get(id);
            if (channel != null) {
                channel.handle(payload, connection);
            } else {
                log.error("Received packet for unknown channel {} {} {}", id, connection, payload);
                connection.write(new S2CChannelStatsPacket(id, false));
            }
        } else if (packet instanceof C2SOpenChannelPacket(String id, ChannelType channelType)) {
            var channel = channels.get(id);
            if (channel != null) {
                connection.write(new S2CChannelStatsPacket(id, true));
                channel.handle(new ClientConnectMessage(connection), connection);
            } else {
                if (channelType == ChannelType.LOCKS) {
                    channel = addChannel(id, c -> {
                        c.pipeline().addLast("locks", new ServerLockHandler());
                    });
                    connection.write(new S2CChannelStatsPacket(id, true));
                    channel.handle(new ClientConnectMessage(connection), connection);
                } else {
                    log.error("Trying to open unsupported channel type! {} {} {}", id, connection, channelType);
                    connection.write(new S2CChannelStatsPacket(id, false));
                }
            }
        }
    }

    public void onDisconnect(Connection connection) {
        for (ServerChannel serverChannel : List.copyOf(channels.values())) {
            serverChannel.handle(new ClientDisconnectMessage(connection), connection);
        }
    }

    public void close() {
        for (ServerChannel serverChannel : List.copyOf(channels.values())) {
            try {
                serverChannel.pipeline().closeAll().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to close channel {}", serverChannel.id(), e);
            }
        }
    }

    public long receivedPacketsSumThenReset() {
        return receivedPackets.sumThenReset();
    }
}
