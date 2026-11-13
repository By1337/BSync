package dev.by1337.sync.client.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Config {
    public final String id;
    public final int workers;
    public final int netty_threads;
    public final Map<String, ConnectionConfig> servers;
    public final Map<String, String> aliases;
    public final Map<String, List<String>> groupAliases;

    public Config(String id, int workers, int nettyThreads, Map<String, ConnectionConfig> servers) {
        this(id, workers, nettyThreads, servers, Map.of());
    }

    public Config(String id, int workers, int nettyThreads, Map<String, ConnectionConfig> servers,
        Map<String, String> aliases) {
        this(id, workers, nettyThreads, servers, aliases, Map.of());
    }

    public Config(String id, int workers, int nettyThreads, Map<String, ConnectionConfig> servers,
        Map<String, String> aliases, Map<String, List<String>> groupAliases) {
        this.id = Objects.requireNonNull(id);
        this.workers = workers;
        netty_threads = nettyThreads;
        if (nettyThreads >= 0) {
            System.setProperty("bcsync.client.netty.threads", Integer.toString(nettyThreads));
        }
        this.servers = Collections.unmodifiableMap(Objects.requireNonNull(servers));
        this.aliases = Map.copyOf(aliases);
        for (var entry : this.aliases.entrySet()) {
            String alias = entry.getKey();
            String target = entry.getValue();
            if (alias.isBlank()) {
                throw new IllegalArgumentException("BSync connection alias must not be blank");
            }
            if (servers.containsKey(alias)) {
                throw new IllegalArgumentException("BSync connection alias conflicts with server: " + alias);
            }
            if (!servers.containsKey(target)) {
                throw new IllegalArgumentException("Unknown BSync server for alias " + alias + ": " + target);
            }
        }
        var groups = new HashSet<String>();
        for (ConnectionConfig server : servers.values()) {
            if (server.group() != null) groups.add(server.group());
        }
        Map<String, List<String>> preparedGroupAliases = new HashMap<>();
        for (var entry : groupAliases.entrySet()) {
            String alias = Objects.requireNonNull(entry.getKey());
            List<String> targets = List.copyOf(entry.getValue());
            if (alias.isBlank()) {
                throw new IllegalArgumentException("BSync group alias must not be blank");
            }
            if (groups.contains(alias)) {
                throw new IllegalArgumentException("BSync group alias conflicts with group: " + alias);
            }
            if (targets.isEmpty()) {
                throw new IllegalArgumentException("BSync group alias must include at least one group: " + alias);
            }
            for (String target : targets) {
                if (!groups.contains(target)) {
                    throw new IllegalArgumentException("Unknown BSync group for alias " + alias + ": " + target);
                }
            }
            preparedGroupAliases.put(alias, targets);
        }
        this.groupAliases = Map.copyOf(preparedGroupAliases);
    }

    public String resolveConnectionName(String name) {
        return aliases.getOrDefault(name, name);
    }

    public List<String> resolveGroupNames(String name) {
        return groupAliases.getOrDefault(name, List.of(name));
    }
}
