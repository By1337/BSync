package dev.by1337.sync.bukkit;

import dev.by1337.core.util.io.ResourceUtil;
import dev.by1337.sync.bukkit.test.TestInvSync;
import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.security.Ed25519;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class BSync extends JavaPlugin {
    private static final Logger log = LoggerFactory.getLogger(BSync.class);
    private static File homeDir;
    private static Config config;
    private static Map<String, Connection> connections = new HashMap<>();

    @Override
    public void onLoad() {
        homeDir = getDataFolder();
        var res = Decoders.CONFIG_DECODER.decode(ResourceUtil.load("config.yml", this).get(), homeDir);
        config = res.result();
        if (config == null) {
            throw new IllegalStateException("Failed to load config\n" + res.error());
        }
        if (res.hasError()) {
            getSLF4JLogger().error(res.error());
        }
        EventLoopWorkers workers = new EventLoopWorkers("bsync-worker-%d", config.workers);
        ClientBootstrap clientBootstrap = new ClientBootstrap();
        for (Map.Entry<String, ConnectionConfig> entry : config.servers.entrySet()) {
            Connection connection = new Connection(
                    entry.getValue(),
                    workers,
                    config.id,
                    clientBootstrap
            );
            connections.put(entry.getKey(), connection);
        }
        for (Connection value : connections.values()) {
            value.connect();
            int x = 0;
            while (!value.hasConnection() && !Bukkit.isStopping()) {
                LockSupport.parkNanos(1_000_000L);
                if (++x >= 10_000) {
                    getSLF4JLogger().error("Failed to connect to {}", value.config());
                    Bukkit.shutdown();
                    return;
                }
            }
        }
    }

    @Override
    public void onEnable() {
        // ChannelMaker.createDataChannel(getConnection("example"), "test-inv", locks -> new TestInvSync(locks, this));
        //Player pl;
        //pl.getInventory().clear();
    }

    @Override
    public void onDisable() {
        connections.values().forEach(c -> {
            try {
                c.close().get(15, TimeUnit.SECONDS);
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                log.error("Failed to close connection", e);
            }
        });
        connections.clear();
    }

    public static Connection getConnection(String name) {
        return Objects.requireNonNull(connections.get(name), "Unknown server: " + name);
    }
    public static List<Connection> getGroup(String group){
        List<Connection> result = new ArrayList<>();
        for (Connection value : connections.values()) {
            if (value.config().group().equals(group)){
                result.add(value);
            }
        }
        if (result.isEmpty()) throw new IllegalStateException("Unknown server group " + group);
        return result;
    }
}
