package dev.by1337.sync.client.network;

import dev.by1337.sync.client.channel.ClientChannel;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.ChannelRegistryContext;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.PingPacket;
import dev.by1337.sync.common.packet.impl.PongPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SCloseChannelPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SOpenChannelPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CChannelStatsPacket;
import dev.by1337.sync.common.util.BSUtils;
import dev.by1337.sync.common.util.SingleSemaphore;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private ConnectionHandler connection;
    private final Map<String, ClientChannel> channels = new ConcurrentHashMap<>();
    private final Map<ClientChannel, CompletableFuture<Void>> removals = new ConcurrentHashMap<>();
    private long ping = 0;
    private final SingleSemaphore reconnectSemaphore = new SingleSemaphore();
    private final EventLoopWorker executor;
    private final ChannelRegistryContext registryContext;

    public Connection(ConnectionConfig config, EventLoopWorkers workers, String id, ClientBootstrap bootstrap) {
        this.config = config;
        this.workers = workers;
        this.id = id;
        this.bootstrap = bootstrap;
        registryContext = new ChannelRegistryContext();
        executor = workers.getNext();
        pingTask(executor);
    }

    private void pingTask(EventLoopWorker worker) {
        if (hasConnection()) {
            write(new PingPacket());
        }
        worker.schedule(() -> pingTask(worker), 3000);
    }

    public void connect() {
        this.connection = new ConnectionHandler(id, config, bootstrap, this);
        this.connection.connect(c -> c.attr(ChannelRegistryContext.CHANNEL_REGISTRY_CONTEXT).set(registryContext));
    }

    /**
     * Removes the channel after closing all its handlers. External callers wait for
     * completion; a reentrant call from the channel's own cleanup callback must not
     * wait for itself. Concurrent removals share the same completion. Calls on the
     * transport loop or while holding a handler monitor are also nonblocking;
     * use removeChannelAsync() to observe completion without holding such resources.
     */
    public void removeChannel(String id) {
        var channel = channels.get(id);
        if (channel == null) return;
        var removal = removeChannel(channel);
        var conn = connection;
        var socket = conn == null ? null : conn.channel();
        // Completion also needs the transport loop to flush queued cleanup packets.
        if (channel.eventLoop().isWorkerThread() || socket != null && socket.eventLoop().inEventLoop()) return;
        for (var entry : channel.pipeline().getHandlers()) {
            if (Thread.holdsLock(entry.handler())) {
                log.warn("Channel {} removal is asynchronous: caller holds the monitor of handler {}. "
                        + "Release that monitor before awaiting removeChannelAsync().", id, entry.name());
                return;
            }
        }
        try {
            removal.get(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while removing channel " + id, e);
        } catch (Exception e) {
            log.error("Error closing channel {}", id, e);
        }
    }

    /** Completes after all handlers, their queued cleanup and the registration are closed. */
    public CompletableFuture<Void> removeChannelAsync(String id) {
        var channel = channels.get(id);
        return channel == null ? CompletableFuture.completedFuture(null) : removeChannel(channel);
    }

    private CompletableFuture<Void> removeChannel(ClientChannel channel) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        var previous = removals.putIfAbsent(channel, result);
        if (previous != null) return previous;
        channel.close().whenComplete((ignored, error) -> {
            var conn = connection;
            var socket = conn == null ? null : conn.channel();
            Runnable detach = () -> {
                try {
                    if (channels.get(channel.id()) == channel) {
                        if (conn != null && conn.authorized()) {
                            conn.send(new C2SCloseChannelPacket(channel.id()));
                            if (socket != null) BSUtils.safe(socket::flush);
                        }
                        registryContext.onChannelClose(channel.id());
                        channels.remove(channel.id(), channel);
                    }
                    if (error == null) result.complete(null);
                    else result.completeExceptionally(error);
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                } finally {
                    removals.remove(channel, result);
                }
            };
            if (socket == null || socket.eventLoop().inEventLoop()) detach.run();
            else {
                try {
                    socket.eventLoop().execute(detach);
                } catch (RejectedExecutionException stopped) {
                    detach.run();
                }
            }
        });
        return result;
    }

    public ClientChannel addChannel(String id, String channelType, Consumer<ClientChannel> init) {
        return addChannel(id, channelType, init, workers.getNext());
    }

    public ClientChannel addChannel(String id, String channelType, Consumer<ClientChannel> init, EventLoopWorker worker) {
        if (closing.get()) throw new IllegalStateException("Connection is closed");
        if (channels.get(id) != null) {
            throw new IllegalArgumentException("Channel with id " + id + " already exists");
        }
        ClientChannel channel = new ClientChannel(
                this, id, worker, channelType
        );
        init.accept(channel);
        if (channels.putIfAbsent(id, channel) != null) {
            throw new IllegalArgumentException("Channel with id " + id + " already exists");
        }
        var packets = channel.buildPacketRegistries();
        registryContext.onChannelOpen(id, packets);
        write(new C2SOpenChannelPacket(channel.id(), channel.getChannelType(), packets.createSnapshot()));
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
                    executor.schedule(() -> {
                        //todo пакет и потеряться может
                        if (channels.get(channel.id()) == channel)
                            write(new C2SOpenChannelPacket(channel.id(), channel.getChannelType(), registryContext.getByChannel(channel.id()).createSnapshot()));
                    }, 1_000);
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
            // log.info("ping {}", ping);
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
                this.connection.connect(c -> c.attr(ChannelRegistryContext.CHANNEL_REGISTRY_CONTEXT).set(registryContext));
            }, 2, TimeUnit.SECONDS);
        }
    }

    void postLogin(ConnectionHandler connection) {
        log.info("Connected to {}:{}", config.ip(), config.port());
        onChannelActive();
    }

    private void onChannelActive() {
        for (ClientChannel channel : List.copyOf(channels.values())) {
            write(new C2SOpenChannelPacket(channel.id(), channel.getChannelType(), registryContext.getByChannel(channel.id()).createSnapshot()));
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
            return closeFuture;
        }

        var conn = connection;

        List<CompletableFuture<?>> futures = new ArrayList<>();

        for (ClientChannel channel : List.copyOf(channels.values())) {
            futures.add(removeChannel(channel));
        }

        CompletableFuture
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
                    if (ex == null) closeFuture.complete(null);
                    else closeFuture.completeExceptionally(ex);
                });
        return closeFuture;
    }

    public String mailboxIdentity() {
        return id + "\n" + config.ip() + ":" + config.port();
    }

    public String legacyMailboxIdentity() {
        return id;
    }

    public ConnectionConfig config() {
        return config;
    }

    public long ping() {
        return ping;
    }
}
