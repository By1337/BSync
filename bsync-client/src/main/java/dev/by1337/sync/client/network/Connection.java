package dev.by1337.sync.client.network;

import dev.by1337.sync.client.channel.ClientChannel;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.PingPacket;
import dev.by1337.sync.common.packet.impl.PongPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SCloseChannelPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SOpenChannelPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CChannelStatsPacket;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Connection {
    private static final ScheduledExecutorService RECONNECT_SHEDULER = new ScheduledThreadPoolExecutor(1);
    private static final Logger log = LoggerFactory.getLogger(Connection.class);
    private final ConnectionConfig config;
    private final EventLoopWorkers workers;
    private final String id;
    private final ClientBootstrap bootstrap;
    private volatile boolean closed = false;
    private ConnectionHandler connection;
    private final Map<String, ClientChannel> channels = new ConcurrentHashMap<>();
    private long ping = 0;

    public Connection(ConnectionConfig config, EventLoopWorkers workers, String id, ClientBootstrap bootstrap) {
        this.config = config;
        this.workers = workers;
        this.id = id;
        this.bootstrap = bootstrap;
        pingTask();
    }

    private void pingTask() {
        if (hasConnection()) {
            send(new PingPacket());
        }
        //  worker.schedule(this::pingTask, 1500);
    }

    public void connect() {
        this.connection = new ConnectionHandler(id, config, bootstrap, this);
        this.connection.connect();
    }

    public void removeChannel(String id) {
        var v = channels.remove(id);
        if (v != null) {
            v.close();
        }
    }

    public void addChannel(String id, ChannelType channelType, Consumer<ClientChannel> init) {
        if (closed) throw new IllegalStateException("Connection is closed");
        if (channels.containsKey(id)) {
            throw new IllegalArgumentException("Channel with id " + id + " already exists");
        }
        ClientChannel channel = new ClientChannel(
                this, id, workers.getRandom(), channelType
        );
        init.accept(channel);
        channels.put(id, channel);
        send(new C2SOpenChannelPacket(channel.id(), channel.getChannelType()));
    }

    public void onReceive(Packet packet) {
        if (packet instanceof S2CChannelStatsPacket stats) {
            var channel = channels.get(stats.id);
            if (channel == null) {
                log.error("No channel with id {} found {}", stats.id, packet);
                send(new C2SCloseChannelPacket(stats.id));
            } else {
                channel.onRegister();
            }
        } else if (packet instanceof ChanneledPacket c) {
            var channel = channels.get(c.id);
            if (channel == null) {
                log.error("No channel with id {} found {}", c.id, packet);
            } else {
                channel.handle(packet);
            }
        } else if (packet instanceof PingPacket) {
            send(new PongPacket(System.currentTimeMillis()));
        } else if (packet instanceof PongPacket p) {
            ping = System.currentTimeMillis() - p.timestamp;
            // log.info("ping {}", ping);
        } else {
            log.error("Packet received unknown packet {}", packet);
        }

    }


    void onClosed(ConnectionHandler connection) {
        this.connection = null;
        if (closed) return;
        for (ClientChannel channel : List.copyOf(channels.values())) {
            channel.onChannelInactive();
        }
        log.error("Connection closed {}:{}", config.ip, config.port);
        RECONNECT_SHEDULER.schedule(() -> {
            log.info("Reconnecting {}:{}", config.ip, config.port);
            this.connection = new ConnectionHandler(id, config, bootstrap, this);
            this.connection.connect();
        }, 2, TimeUnit.SECONDS);
    }

    void postLogin(ConnectionHandler connection) {
        log.info("Connected to {}:{}", config.ip, config.port);
        onChannelActive();
    }

    private void onChannelActive() {
        for (ClientChannel channel : List.copyOf(channels.values())) {
            channel.onChannelInactive();
        }
    }

    public void send(Packet packet) {
        var conn = connection;
        if (conn != null && conn.connected()) {
            conn.send(packet);
        }
    }

    public boolean hasConnection() {
        return connection != null && connection.connected();
    }

    public void close() {
        if (closed) return;
        closed = true;
        var conn = connection;
        for (ClientChannel channel : List.copyOf(channels.values())) {
            channel.close();
            if (conn != null && conn.connected()) {
                conn.send(new C2SCloseChannelPacket(channel.id()));
            }
        }
        channels.clear();
    }

    public ConnectionConfig config() {
        return config;
    }

    public long ping() {
        return ping;
    }
}
