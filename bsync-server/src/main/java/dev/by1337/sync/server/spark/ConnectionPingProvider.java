package dev.by1337.sync.server.spark;

import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.network.Connection;
import me.lucko.spark.common.monitor.ping.PlayerPingProvider;

import java.util.HashMap;
import java.util.Map;

public class ConnectionPingProvider implements PlayerPingProvider {
    private final DedicatedServer server;

    public ConnectionPingProvider(DedicatedServer server) {
        this.server = server;
    }

    @Override
    public Map<String, Integer> poll() {
        Map<String, Integer> map = new HashMap<>();
        int x = 0;
        for (Connection connection : server.clientList().connections()) {
            if (map.containsKey(connection.id())){
                map.put(connection.id() + ":" + x++, connection.ping());
            }else {
                map.put(connection.id(), connection.ping());
            }
        }
        return map;
    }
}
