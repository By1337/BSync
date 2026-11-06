package dev.by1337.sync.velocity;

import com.google.inject.Inject;
import com.velocitypowered.api.event.EventTask;
import com.velocitypowered.api.event.PostOrder;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.slf4j.Logger;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Plugin(id = "bsync", name = "BSync", version = "1.6", authors = { "by1337" })
public final class BSync {
    private static volatile BSync instance;
    private final Logger logger;
    private final Path directory;
    private Config config;
    private EventLoopWorkers workers;
    private Map<String, Connection> connections = Map.of();

    @Inject
    public BSync(Logger logger, @DataDirectory Path directory) {
        this.logger = logger;
        this.directory = directory;
    }

    @Subscribe(order = PostOrder.FIRST)
    public void initialize(ProxyInitializeEvent event) throws Exception {
        config = ConfigLoader.load(directory);
        workers = new EventLoopWorkers("bsync-velocity-worker-%d", config.workers);
        var bootstrap = new ClientBootstrap();
        Map<String, Connection> prepared = new LinkedHashMap<>();
        for (var entry : config.servers.entrySet()) {
            prepared.put(entry.getKey(), new Connection(entry.getValue(), workers, config.id, bootstrap));
        }
        connections = Collections.unmodifiableMap(prepared);
        instance = this;
        // No blocking wait or proxy shutdown when a backend is unavailable.
        for (Connection connection : connections.values()) connection.connect();
        logger.info("BSync client {} initialized with {} connection(s)", config.id, connections.size());
    }

    @Subscribe(order = PostOrder.LAST)
    public EventTask shutdown(ProxyShutdownEvent event) {
        if (instance == this) instance = null;
        return EventTask.resumeWhenComplete(closeConnections(connections.values()));
    }

    static CompletableFuture<Void> closeConnections(Collection<Connection> connections) {
        return CompletableFuture.allOf(connections.stream().map(Connection::close).toArray(CompletableFuture[]::new));
    }

    private static BSync active() {
        return Objects.requireNonNull(instance, "BSync has not initialized or is shutting down");
    }

    public static String getServerId() {
        return active().config.id;
    }

    public static Connection getConnection(String name) {
        return Objects.requireNonNull(active().connections.get(name), "Unknown BSync connection: " + name);
    }

    public static List<Connection> getGroup(String group) {
        List<Connection> result = active().connections.values().stream().filter(c -> c.config().group().equals(group)).toList();
        if (result.isEmpty()) throw new IllegalStateException("Unknown BSync group: " + group);
        return result;
    }

    public static EventLoopWorkers workers() {
        return active().workers;
    }

    public static Map<String, Connection> connections() {
        return active().connections;
    }
}
