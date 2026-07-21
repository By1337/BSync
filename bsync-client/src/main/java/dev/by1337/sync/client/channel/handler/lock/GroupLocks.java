package dev.by1337.sync.client.channel.handler.lock;

import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.ServerGroup;
import dev.by1337.sync.client.network.Connection;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

public class GroupLocks implements Locks {

    private final ServerGroup<Locks> group;

    public GroupLocks(List<Connection> g, String id, LockManager manager) {
        group = new ServerGroup<>(g, c -> ChannelMaker.createLocks(c, id, manager));
    }

    @Override
    public boolean isLocked(UUID uuid) {
        return group.route(uuid).isLocked(uuid);
    }

    @Override
    public void pushMail(UUID key, String json) {
        group.route(key).pushMail(key, json);
    }

    @Override
    public void pushSnapshot(UUID key, byte[] snapshot) {
        group.route(key).pushSnapshot(key, snapshot);
    }

    @Override
    public void unlock(UUID key, int version) {
        group.route(key).unlock(key, version);
    }

    @Override
    public int lockAndLoadData(UUID key, BiConsumer<LockStatus, byte @Nullable []> callback) {
        return group.route(key).lockAndLoadData(key, callback);
    }

    @Override
    public boolean isReady() {
        for (ChannelMaker.ChannelData<Locks> channel : group.channels()) {
            if (channel.get().isReady()) return true;
        }
        return false;
    }

    public void close() {
        for (ChannelMaker.ChannelData<Locks> channel : group.channels()) {
            channel.close();
        }
    }
}
