package dev.by1337.sync.client.network;

import dev.by1337.sync.client.channel.ClientChannel;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.PingPacket;
import dev.by1337.sync.common.packet.impl.PongPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SCloseChannelPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SOpenChannelPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CChannelStatsPacket;
import dev.by1337.sync.common.util.SingleSemaphore;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class Connection implements SocketConnection {
    private static final ScheduledExecutorService RECONNECT_SHEDULER = new ScheduledThreadPoolExecutor(1);
    private static final Logger log = LoggerFactory.getLogger(Connection.class);
    private final ConnectionConfig config;
    private final EventLoopWorkers workers;
    private final String id;
    private final ClientBootstrap bootstrap;
    private final AtomicBoolean closing = new AtomicBoolean();
    private ConnectionHandler connection;
    private final Map<String, ClientChannel> channels = new ConcurrentHashMap<>();
    private long ping = 0;
    private final SingleSemaphore reconnectSemaphore = new SingleSemaphore();

    public Connection(ConnectionConfig config, EventLoopWorkers workers, String id, ClientBootstrap bootstrap) {
        this.config = config;
        this.workers = workers;
        this.id = id;
        this.bootstrap = bootstrap;
        pingTask(workers.getNext());
    }

    private void pingTask(EventLoopWorker worker) {
        if (hasConnection()) {
            write(new PingPacket());
        }
        worker.schedule(() -> pingTask(worker), 3000);
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

    public ClientChannel addChannel(String id, ChannelType channelType, Consumer<ClientChannel> init) {
        if (closing.get()) throw new IllegalStateException("Connection is closed");
        if (channels.containsKey(id)) {
            throw new IllegalArgumentException("Channel with id " + id + " already exists");
        }
        ClientChannel channel = new ClientChannel(
                this, id, workers.getNext(), channelType
        );
        init.accept(channel);
        channels.put(id, channel);
        write(new C2SOpenChannelPacket(channel.id(), channel.getChannelType()));
        return channel;
    }

    public void onReceive(Packet packet) {
        // System.out.println("CLIENT IN " + packet);
        if (packet instanceof S2CChannelStatsPacket stats) {
            var channel = channels.get(stats.id());
            if (channel == null) {
                log.error("No channel with id {} found {}", stats.id(), packet);
                write(new C2SCloseChannelPacket(stats.id()));
            } else {
                //пупупу хзхз
                if (stats.opened()) {
                    channel.onRegister();
                    channel.onChannelActive();
                } else {
                    channel.onChannelInactive();
                    log.error("Channel {} has been closed by server. Try to reopen", stats.id());
                    write(new C2SOpenChannelPacket(channel.id(), channel.getChannelType()));
                }
            }
        } else if (packet instanceof ChanneledPacket c) {
            var channel = channels.get(c.id());
            if (channel == null) {
                log.error("No channel with id {} found {}", c.id(), packet);
            } else {
                channel.handle(c.payload());
            }
        } else if (packet instanceof PingPacket) {
            write(new PongPacket(System.currentTimeMillis()));
        } else if (packet instanceof PongPacket p) {
            ping = System.currentTimeMillis() - p.timestamp();
            log.info("ping {}", ping);
        } else {
            log.error("Packet received unknown packet {}", packet);
        }
    }

    void onClosed(ConnectionHandler connection) {
        this.connection = null;
        if (closing.get()) return;
        for (ClientChannel channel : List.copyOf(channels.values())) {
            channel.onChannelInactive();
        }
        log.error("Connection closed {}:{}", config.ip(), config.port());
        if (reconnectSemaphore.tryAcquire()) {
            RECONNECT_SHEDULER.schedule(() -> {
                reconnectSemaphore.release();
                log.info("Reconnecting {}:{}", config.ip(), config.port());
                this.connection = new ConnectionHandler(id, config, bootstrap, this);
                this.connection.connect();
            }, 2, TimeUnit.SECONDS);
        }
    }

    void postLogin(ConnectionHandler connection) {
        log.info("Connected to {}:{}", config.ip(), config.port());
        onChannelActive();
    }

    private void onChannelActive() {
        for (ClientChannel channel : List.copyOf(channels.values())) {
            write(new C2SOpenChannelPacket(channel.id(), channel.getChannelType()));
        }
    }

    @Override
    public void write(Packet packet) {
        var conn = connection;
        if (conn != null && conn.authorized()) {
            conn.send(packet);
        }
    }

    public boolean hasConnection() {
        return connection != null && connection.authorized();
    }


    public CompletableFuture<Void> close() {
        if (!closing.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }

        var conn = connection;

        List<CompletableFuture<?>> futures = new ArrayList<>();

        if (conn != null && conn.authorized()) {
            for (ClientChannel channel : List.copyOf(channels.values())) {
                futures.add(channel.close());
                // хотим закрыть канал после того как все таски запущенные из close выполняться
                channel.eventLoop().execute(() -> conn.send(new C2SCloseChannelPacket(channel.id())));
            }
        }

        return CompletableFuture
                .allOf(futures.toArray(CompletableFuture[]::new))
                .orTimeout(30, TimeUnit.SECONDS)
                .exceptionally(ex -> null)
                .thenCompose(v -> {
                    connection = null;

                    if (conn != null) {
                        return conn.close();
                    }

                    return CompletableFuture.completedFuture(null);
                })
                .whenComplete((v, ex) -> {
                    channels.clear();
                });
    }

    public ConnectionConfig config() {
        return config;
    }

    public long ping() {
        return ping;
    }

    public @Nullable UUID serverUid() {
        var con = connection;
        if (con == null) return null;
        return con.serverUid();
    }
}
