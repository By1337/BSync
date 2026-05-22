package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SCloseChannelPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SOpenChannelPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CChannelStatsPacket;
import dev.by1337.sync.common.work.EventLoopWorkers;
import dev.by1337.sync.server.channel.handler.ServerLockHandler;
import dev.by1337.sync.server.channel.messages.ClientConnectMessage;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import dev.by1337.sync.server.network.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ChannelManager {
    private static final Logger log = LoggerFactory.getLogger(ChannelManager.class);
    private final EventLoopWorkers workers;
    private final Map<String, ServerChannel> channels = new ConcurrentHashMap<>();

    public ChannelManager(EventLoopWorkers workers) {
        this.workers = workers;
    }

    public ServerChannel addChannel(String id, Consumer<ServerChannel> init) {
        if (channels.containsKey(id)) {
            throw new IllegalArgumentException("Channel with id " + id + " already exists");
        }
        ServerChannel serverChannel = new ServerChannel(
                id,
                workers.getNext()
        );
        init.accept(serverChannel);
        channels.put(id, serverChannel);
        serverChannel.onRegister();
        return serverChannel;
    }

    public void onReceive(Packet packet, Connection connection) {
      //  System.out.println("SERVER IN " + packet);
        if (packet instanceof C2SCloseChannelPacket close) {
            var channel = channels.get(close.id);
            if (channel != null) {
                channel.handle(new ClientDisconnectMessage(connection), connection);
            } else {
                log.error("Try to close unknown channel {} {}", close.id, connection);
            }
        } else if (packet instanceof ChanneledPacket c) {
            var channel = channels.get(c.id);
            if (channel != null) {
                channel.handle(c.payload, connection);
            } else {
                log.error("Received packet for unknown channel {} {} {}", c.id, connection, c.payload);
                connection.write(new S2CChannelStatsPacket(c.id, false));
            }
        } else if (packet instanceof C2SOpenChannelPacket open) {
            var channel = channels.get(open.id);
            if (channel != null) {
                connection.write(new S2CChannelStatsPacket(open.id, true));
                channel.handle(new ClientConnectMessage(connection), connection);
            } else {
                if (open.channelType == ChannelType.DATA_CHANNEL) {
                    channel = addChannel(open.id, c -> {
                        c.pipeline().addLast("locks", new ServerLockHandler());
                    });
                    connection.write(new S2CChannelStatsPacket(open.id, true));
                    channel.handle(new ClientConnectMessage(connection), connection);
                } else {
                    log.error("Trying to open unsupported channel type! {} {} {}", open.id, connection, open.channelType);
                    connection.write(new S2CChannelStatsPacket(open.id, false));
                }
            }
        }
    }

    public void onDisconnect(Connection connection) {
        for (ServerChannel serverChannel : List.copyOf(channels.values())) {
            serverChannel.handle(new ClientDisconnectMessage(connection), connection);
        }
    }
}
