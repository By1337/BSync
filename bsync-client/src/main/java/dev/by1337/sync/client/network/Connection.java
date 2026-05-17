package dev.by1337.sync.client.network;

import dev.by1337.sync.client.channel.AbstractChannel;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.work.ConnectionWorker;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.PingPacket;
import dev.by1337.sync.common.packet.impl.PongPacket;
import dev.by1337.sync.common.util.SingleSemaphore;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Connection {
    private static final ScheduledExecutorService RECONNECT_SHEDULER = new ScheduledThreadPoolExecutor(1);
    private static final Logger log = LoggerFactory.getLogger(Connection.class);
    private final ConnectionWorker worker;
    private final ConnectionConfig config;
    private final String id;
    private final ClientBootstrap bootstrap;
    private volatile boolean closed = false;
    private ConnectionHandler connection;
    private final Object2ObjectOpenHashMap<String, AbstractChannel> channels = new Object2ObjectOpenHashMap<>();
    private final SingleSemaphore drainPackets = new SingleSemaphore();
    private final MpscArrayQueue<Packet> incomingPacketsQueue = new MpscArrayQueue<>(4096);
    private long ping = 0;

    public Connection(ConnectionWorker worker, ConnectionConfig config, String id, ClientBootstrap bootstrap) {
        this.worker = worker;
        this.config = config;
        this.id = id;
        this.bootstrap = bootstrap;
        pingTask();
    }

    private void pingTask() {
        if (hasConnection()) {
            send(new PingPacket());
        }
        worker.schedule(this::pingTask, 1500);
    }

    public void connect() {
        this.connection = new ConnectionHandler(id, config, bootstrap, this);
        this.connection.connect();
    }

    public void removeChannel(AbstractChannel channel) {
        worker.execute(() -> {
            channels.remove(channel.id());
            channel.onUnregister();
        });
    }

    public void addChannel(AbstractChannel channel) {
        if (closed) throw new IllegalStateException("Connection is closed");
        worker.execute(() -> {
            channels.put(channel.id(), channel);
            channel.onRegister();
        });
    }

    public void onReceive(Packet packet) {
        if (!incomingPacketsQueue.add(packet)) {
            log.error("Packet queue is full {}", packet, new Throwable());
        } else {
            if (drainPackets.tryAcquire()) {
                worker.schedule(() -> {
                    drainPackets.release();
                    drainPackets();
                }, 2);
            }
        }

    }

    private void drainPackets() {
        Packet packet;
        while ((packet = incomingPacketsQueue.poll()) != null) {
            if (packet instanceof ChanneledPacket c) {
                var channel = channels.get(c.id);
                try {
                    channel.receive(packet);
                } catch (Exception e) {
                    log.error("Error while receiving packet from channel {} {}", channel.id(), packet, e);
                }
            } else if (packet instanceof PingPacket) {
                send(new PongPacket(System.currentTimeMillis()));
            } else if (packet instanceof PongPacket p) {
                ping = System.currentTimeMillis() - p.timestamp;
                log.info("ping {}", ping);
            } else {
                log.error("Packet received unknown packet {}", packet);
            }
        }
    }

    void onClosed(ConnectionHandler connection) {
        this.connection = null;
        if (closed) return;
        log.error("Connection closed {}:{}", config.ip, config.port);
        RECONNECT_SHEDULER.schedule(() -> {
            log.info("Reconnecting {}:{}", config.ip, config.port);
            this.connection = new ConnectionHandler(id, config, bootstrap, this);
            this.connection.connect();
        }, 2, TimeUnit.SECONDS);
    }

    void postLogin(ConnectionHandler connection) {
        log.info("Connected to {}:{}", config.ip, config.port);
    }

    public void send(Packet packet) {
        if (hasConnection()) {
            connection.send(packet);
        }
    }

    public boolean hasConnection() {
        return connection != null && connection.connected();
    }

    public void close() {
        if (closed) return;
        closed = true;
        worker.execute(() -> {
            channels.values().forEach(AbstractChannel::onClose);
            channels.clear();
            if (connection != null) {
                if (connection.connected()) {

                }
                connection.close();
            }
        });
    }

    public ConnectionConfig config() {
        return config;
    }

    public void execute(Runnable runnable) {
        worker.execute(runnable);
    }

    public void schedule(Runnable runnable, long ms) {
        worker.schedule(runnable, ms);
    }

    public boolean isWorkerThread() {
        return worker.isWorkerThread();
    }

    public long ping() {
        return ping;
    }
}
