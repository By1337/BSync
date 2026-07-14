package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.channel.handler.lock.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.lock.GroupLocks;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.client.channel.handler.pub.ClientPublisherHandler;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.channel.handler.request.RequestsHandler;

import java.util.List;
import java.util.function.Consumer;

public class ChannelMaker {

    public static Locks createGroupLocks(List<Connection> g, String id, LockManager manager) {
        return new GroupLocks(g, id, manager);
    }

    public static Locks createLocks(Connection c, String id, LockManager manager) {
        var v = c.addChannel(id, ChannelType.LOCKS, cc -> {
            var locks = new ClientLocksHandler();
            locks.lockManager(manager);
            cc.pipeline()
                    //.addLast("requests", new RequestsHandler())
                    .addLast("locks", locks);
        });
        return (ClientLocksHandler) v.pipeline().getHandler("locks");
    }

    public static Consumer<byte[]> createPublisher(Connection c, String id, Consumer<byte[]> reader) {
        var v = c.addChannel(id, ChannelType.PUBLISHER, cc -> {
            cc.pipeline()
                   // .addLast("requests", new RequestsHandler())
                    .addLast("publisher", new ClientPublisherHandler(reader));
        });
        return (ClientPublisherHandler) v.pipeline().getHandler("publisher");
    }
}
