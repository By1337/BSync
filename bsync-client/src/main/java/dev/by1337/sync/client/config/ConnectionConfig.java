package dev.by1337.sync.client.config;

public class ConnectionConfig {
    public final String group;
    public final String ip;
    public final int port;
    public final String keyPath;

    public ConnectionConfig(String group, String ip, int port, String keyPath) {
        this.group = group;
        this.ip = ip;
        this.port = port;
        this.keyPath = keyPath;
    }
}
