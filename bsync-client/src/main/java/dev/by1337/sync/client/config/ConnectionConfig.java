package dev.by1337.sync.client.config;

import java.io.File;
import java.util.Objects;

public class ConnectionConfig {
    public final String ip;
    public final int port;
    public final File keyPath;

    public ConnectionConfig(String ip, int port, File keyPath) {
        this.ip = Objects.requireNonNull(ip);
        this.port = port;
        this.keyPath = Objects.requireNonNull(keyPath);
    }

    @Override
    public String toString() {
        return "ConnectionConfig{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", keyPath=" + keyPath +
                '}';
    }
}
