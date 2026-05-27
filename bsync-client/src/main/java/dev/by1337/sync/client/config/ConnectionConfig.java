package dev.by1337.sync.client.config;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ConnectionConfig {
    private final String group;
    private final String ip;
    private final int port;
    private final String private_key;
    private final String serverId;

    public ConnectionConfig(String group, String ip, int port, String private_key) {
        this.group = group;
        this.ip = Objects.requireNonNull(ip);
        this.port = port;
        this.private_key = Objects.requireNonNull(private_key);
        serverId = Hashing.sha256().hashBytes(private_key.getBytes(StandardCharsets.UTF_8)).toString();
    }

    public String serverId() {
        return serverId;
    }

    public String group() {
        return group;
    }

    public String ip() {
        return ip;
    }

    public int port() {
        return port;
    }

    public String private_key() {
        return private_key;
    }

    @Override
    public String toString() {
        return "ConnectionConfig{" +
                "group='" + group + '\'' +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", private_key='secret'" +
                ", serverId='" + serverId + "'" +
                '}';
    }
}
