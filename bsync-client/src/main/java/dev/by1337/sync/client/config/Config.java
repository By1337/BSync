package dev.by1337.sync.client.config;

import java.util.List;

public class Config {
    public final String id;
    public final List<ConnectionConfig> connections;

    public Config(String id, List<ConnectionConfig> connections) {
        this.id = id;
        this.connections = connections;
    }
}
