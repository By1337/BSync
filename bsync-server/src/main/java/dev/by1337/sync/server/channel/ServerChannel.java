package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.PacketRegistries;
import dev.by1337.sync.common.packet.PacketRegistry;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.messages.ClientConnectMessage;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import dev.by1337.sync.server.network.Connection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
    private final List<PacketRegistry> registries = new ArrayList<>();
    private PacketRegistries packetRegistries;

    public ServerChannel(String id, EventLoopWorker eventLoop, DedicatedServer server) {
        this.id = id;
        this.eventLoop = eventLoop;
        log = LoggerFactory.getLogger(id + "|Channel");
        pipeline = new Pipeline(eventLoop);
        this.server = server;
        registries.add(Packets.BSYNC_MAIN);
    }
    public ServerChannel addRegistries(PacketRegistry... registries) {
        if (packetRegistries != null) throw new IllegalStateException("PacketRegistries already set");
        this.registries.addAll(Arrays.asList(registries));
        return this;
    }

    public PacketRegistries getPacketRegistries() {
        if (this.packetRegistries != null) return packetRegistries;
        PacketRegistries result = new PacketRegistries();
        for (int id = 0; id < registries.size(); id++) {
            var r = registries.get(id);
            result.add(id, r.id(), r);
        }
        return packetRegistries = result;
    }

    public @NotNull PacketRegistries buildPacketRegistries(PacketRegistries.Snapshot snapshot) {
        PacketRegistries result = new PacketRegistries();
        loop:
        for (PacketRegistries.Snapshot.RegistryData data : snapshot.registryData()) {
            for (PacketRegistry registry : registries) {
                if (!registry.id().equals(data.name())) continue;
                if (data.version() > registry.latestVersion()) throw new IllegalArgumentException(registry.id() + " server version=" + registry.latestVersion() + " client version=" + data.version());
                result.add(data.id(), data.name(), registry, data.version());
                continue loop;
            }
            throw new IllegalArgumentException("Unknown registry " + data.name());
        }
        return result;
    }


    public Pipeline pipeline() {
        return pipeline;
    }

    public void handle(ChannelMessage packet, Connection connection) {
        if (packet instanceof ClientConnectMessage(SocketConnection conn, PacketRegistries.Snapshot snapshot)) {
            connections.add(conn);
            connection.onChannelOpen(id, buildPacketRegistries(snapshot));
        } else if (packet instanceof ClientDisconnectMessage(SocketConnection conn)) {
            connections.remove(conn);
            connection.onChannelClose(id);
        }
        pipeline.execute(packet, lookup(connection));
    }

    public void forEachConnections(Consumer<dev.by1337.sync.common.channel.pipeline.Connection> c) {
        for (SocketConnection connection : connections) {
            c.accept(lookup(connection));
        }
    }

    public void write(SocketConnection connection, Packet packet) {
        connection.write(new ChanneledPacket(id, packet));
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
