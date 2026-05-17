package dev.by1337.sync.client.config;

import java.util.Map;

public class Config {
    public final String id;
    private final Map<String, ConnectionConfig> servers;

    public Config(String id, Map<String, ConnectionConfig> servers) {
        this.id = id;
        this.servers = servers;
    }
}
