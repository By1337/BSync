package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.channel.handler.lock.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.client.channel.handler.pub.ClientPublisherHandler;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.packet.Packets;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class ChannelMaker {

    public static ChannelData<Locks> createGroupLocks(List<Connection> g, String id, LockManager manager) {
        return createGroupLocks(g, id, manager, Locks.Type.ALL);
    }

    public static ChannelData<Locks> createGroupLocks(List<Connection> g, String id, LockManager manager, Locks.Type type) {
        return create(
                g,
                type.channelType,
                id,
                cc -> {
                    var locks = new ClientLocksHandler(type);
                    locks.lockManager(manager);
                    cc.pipeline()
                            .addLast("locks", locks);
                    cc.addRegistries(Packets.BSYNC_LOCKS);
                    return locks;
                },
                group -> new Locks() {
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
                    public CompletableFuture<byte @Nullable []> loadSnapshot(UUID key) {
                        return group.route(key).loadSnapshot(key);
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

                    @Override
                    public void loadMails(UUID key) {
                        group.route(key).loadMails(key);
                    }
                }
        );
    }

    public static ChannelData<Locks> createLocks(Connection c, String id, LockManager manager) {
        return createLocks(c, id, manager, Locks.Type.ALL);
    }

    public static ChannelData<Locks> createLocks(Connection c, String id, LockManager manager, Locks.Type type) {

        var v = c.addChannel(id, type.channelType, cc -> {
            var locks = new ClientLocksHandler(type);
            locks.lockManager(manager);
            cc.pipeline()
                    .addLast("locks", locks);
            cc.addRegistries(Packets.BSYNC_LOCKS);
        });
        var result = (ClientLocksHandler) v.pipeline().getHandler("locks");
        return new ChannelData<Locks>() {
            @Override
            public Locks get() {
                return result;
            }

            @Override
            public void close() {
                c.removeChannel(id);
            }
        };
    }

    public static Consumer<byte[]> createPublisher(Connection c, String id, Consumer<byte[]> reader) {
        var v = c.addChannel(id, ChannelType.PUBLISHER, cc -> {
            cc.pipeline()
                    .addLast("publisher", new ClientPublisherHandler(reader));
            cc.addRegistries(Packets.BSYNC_PUBLISH);
        });
        return (ClientPublisherHandler) v.pipeline().getHandler("publisher");
    }

    public interface ChannelData<T> {
        T get();

        void close();
    }

    public static <T> ChannelMaker.ChannelData<T> create(
            Function<String, Connection> byNameConnectionLookup,
            Function<String, List<Connection>> byNameGroupLookup,
            String path,
            String type,
            Function<ClientChannel, T> factory,
            Function<ServerGroup<T>, T> routerFactory
    ) {
        if (path.startsWith("server://")) {
            String[] args = path.substring("server://".length()).split("/");
            if (args.length != 2) throw new IllegalArgumentException("bad bsync path " + path);
            var conn = byNameConnectionLookup.apply(args[0]);
            Ref<T> ref = new Ref<>();
            conn.addChannel(args[1], type, cc -> {
                ref.value = factory.apply(cc);
            });
            Objects.requireNonNull(ref.value);
            return new ChannelMaker.ChannelData<T>() {
                @Override
                public T get() {
                    return ref.value;
                }

                @Override
                public void close() {
                    conn.removeChannel(args[1]);
                }
            };
        } else if (path.startsWith("group://")) {
            String[] args = path.substring("group://".length()).split("/");
            if (args.length != 2) throw new IllegalArgumentException("bad bsync path " + path);
            return create(
                    byNameGroupLookup.apply(args[0]),
                    type,
                    args[1],
                    factory,
                    routerFactory
            );
        } else {
            throw new IllegalArgumentException("bad bsync path " + path);
        }
    }

    public static <T> ChannelMaker.ChannelData<T> create(List<Connection> g, String type, String id, Function<ClientChannel, T> factory, Function<ServerGroup<T>, T> routerFactory) {
        ServerGroup<T> group = new ServerGroup<>(g, c -> {
            Ref<T> ref = new Ref<>();
            c.addChannel(id, type, cc -> {
                ref.value = factory.apply(cc);
            });
            Objects.requireNonNull(ref.value);
            return new ChannelMaker.ChannelData<T>() {
                @Override
                public T get() {
                    return ref.value;
                }

                @Override
                public void close() {
                    c.removeChannel(id);
                }
            };
        });
        T router = routerFactory.apply(group);
        return new ChannelMaker.ChannelData<T>() {
            @Override
            public T get() {
                return router;
            }

            @Override
            public void close() {
                for (ChannelMaker.ChannelData<T> channel : group.channels()) {
                    channel.close();
                }
            }
        };
    }

    private static class Ref<T> {
        T value;
    }
}
