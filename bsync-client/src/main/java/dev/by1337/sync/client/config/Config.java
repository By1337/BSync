package dev.by1337.sync.client.config;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class Config {
    public final String id;
    public final int workers;
    public final int netty_threads;
    public final Map<String, ConnectionConfig> servers;

    public Config(String id, int workers, int nettyThreads, Map<String, ConnectionConfig> servers) {
        this.id = Objects.requireNonNull(id);
        this.workers = workers;
        netty_threads = nettyThreads;
        if (nettyThreads >= 0){
            System.setProperty("bcsync.client.netty.threads", Integer.toString(nettyThreads));
        }
        this.servers = Collections.unmodifiableMap(Objects.requireNonNull(servers));
    }
}
