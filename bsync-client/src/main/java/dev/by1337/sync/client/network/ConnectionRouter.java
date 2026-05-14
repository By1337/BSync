package dev.by1337.sync.client.network;

import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.config.ConnectionConfig;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionRouter {
    private final List<Connection> connections = new CopyOnWriteArrayList<>();
    private final Object2ObjectOpenHashMap<String, List<ConnectionConfig>> connectionsByGroup;
    private final Map<ConnectionConfig, Connection> connectionsByConfig = new ConcurrentHashMap<>();
    private final Config config;

    public ConnectionRouter(Config config) {
        this.config = config;
        connectionsByGroup = new Object2ObjectOpenHashMap<>();
        for (ConnectionConfig connection : config.connections) {
            connectionsByGroup.computeIfAbsent(connection.group, k -> new ArrayList<>()).add(connection);
        }
        for (List<ConnectionConfig> value : connectionsByGroup.values()) {
            value.sort(Comparator.comparingInt(c -> c.balancerKey));
        }
    }

    public @Nullable Connection route(String group, UUID key) {
        var list = connectionsByGroup.get(group);
        if (list == null || list.isEmpty()) throw new IllegalArgumentException("No such group " + group);
        int hash = key.hashCode();
        return connectionsByConfig.get(list.get(hash % list.size()));
    }

    public synchronized void newConnection(Connection connection) {
        if (connections.contains(connection)) return;
        connections.add(connection);
        connectionsByConfig.put(connection.connectionConfig, connection);
    }

    public synchronized void removeConnection(Connection connection) {
        if (!connections.remove(connection)) return;
        connectionsByConfig.remove(connection.connectionConfig);
    }
}
