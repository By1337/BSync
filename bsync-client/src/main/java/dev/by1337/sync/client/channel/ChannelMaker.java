package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.channel.handler.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.LockManager;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.channel.handler.RequestsHandler;

import java.util.function.Function;

public class ChannelMaker {

    public static ClientChannel createDataChannel(Connection c, String id, Function<ClientLocksHandler, LockManager> manager){
        return c.addChannel(id, ChannelType.DATA_CHANNEL, cc -> {
            var locks = new ClientLocksHandler();
            locks.lockManager(manager.apply(locks));
            cc.pipeline()
                    .addLast("requests", new RequestsHandler())
                    .addLast("locks", locks);
        });
    }
}
