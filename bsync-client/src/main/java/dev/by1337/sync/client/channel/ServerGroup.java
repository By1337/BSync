package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.network.Connection;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class ServerGroup<T> {
    private final Object[] servers;
    private final List<ChannelMaker.ChannelData<T>> channels;
    private final AtomicInteger counter = new AtomicInteger();

    public ServerGroup(List<Connection> group, Function<Connection, ChannelMaker.ChannelData<T>> factory) {
        channels = new ArrayList<>();
        var connections = new ArrayList<>(group);
        connections.sort(Comparator.comparing(o -> o.config().serverId()));
        this.servers = new Object[connections.size()];
        for (int i = 0; i < connections.size(); i++) {
            var v = factory.apply(connections.get(i));
            channels.add(v);
            this.servers[i] = v.get();
        }
    }

    public T route(UUID uuid) {
        //noinspection unchecked
        return (T) servers[Math.floorMod(hash(uuid), servers.length)];
    }

    public T next() {
        //noinspection unchecked
        return (T) servers[Math.floorMod(counter.incrementAndGet(), servers.length)];
    }

    static long hash(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }

    public List<ChannelMaker.ChannelData<T>> channels() {
        return channels;
    }

    public void forEach(Consumer<T> c) {
        for (Object server : servers) {
            //noinspection unchecked
            c.accept((T) server);
        }
    }
}
