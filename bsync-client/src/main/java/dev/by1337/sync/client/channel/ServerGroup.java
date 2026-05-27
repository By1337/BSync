package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.network.Connection;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public class ServerGroup<T> {
    private final Object[] servers;

    public ServerGroup(List<Connection> group, Function<Connection, T> factory) {
        var connections = new ArrayList<>(group);
        connections.sort(Comparator.comparing(o -> o.config().serverId()));
        this.servers = new Object[connections.size()];
        for (int i = 0; i < connections.size(); i++) {
            this.servers[i] = factory.apply(connections.get(i));
        }
    }

    public T route(UUID uuid) {
        //noinspection unchecked
        return (T) servers[Math.floorMod(hash(uuid), servers.length)];
    }

    static long hash(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }
}
