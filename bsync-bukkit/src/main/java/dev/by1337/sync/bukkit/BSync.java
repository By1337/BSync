package dev.by1337.sync.bukkit;

import dev.by1337.core.util.io.ResourceUtil;
import dev.by1337.sync.bukkit.test.TestInvSyncHandler;
import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.security.Ed25519;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

public class BSync extends JavaPlugin {
    private static File homeDir;
    private static Config config;
    private static Map<String, Connection> connections = new HashMap<>();

    @Override
    public void onLoad() {
        homeDir = getDataFolder();
        File keysFolder = new File(homeDir, "keys");
        if (!keysFolder.exists()) {
            keysFolder.mkdirs();
            try {
                Path keys = keysFolder.toPath();
                var keyPair = Ed25519.generateKeyPair();
                Files.writeString(keys.resolve("default.pub"), Ed25519.keyToBase64(keyPair.getPublic()), StandardCharsets.UTF_8);
                Files.writeString(keys.resolve("default"), Ed25519.keyToBase64(keyPair.getPrivate()), StandardCharsets.UTF_8);
                try (var in = getResource("keys_readme.txt")) {
                    try (var out = new FileOutputStream(new File(keysFolder, "README.txt"))) {
                        in.transferTo(out);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

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
            while (!value.hasConnection()) {
                LockSupport.parkNanos(1_000_000L);
                if (++x >= 10_000) {
                    getSLF4JLogger().error("Failed to connect to {}", value.config());
                    break;
                }
            }
        }
    }

    @Override
    public void onEnable() {
        getConnection("example").addChannel("test-inv", ChannelType.DATA_CHANNEL, channel -> {
            channel.pipeline().addLast("invs", new TestInvSyncHandler(this));
        });
        //Player pl;
        //pl.getInventory().clear();
    }

    @Override
    public void onDisable() {
        connections.values().forEach(Connection::close);
        connections.clear();
    }

    public static Connection getConnection(String name) {
        return Objects.requireNonNull(connections.get(name), "Unknown server: " + name);
    }
}
