package dev.by1337.sync.server.config;

import dev.by1337.sync.common.security.Ed25519;
import dev.by1337.sync.server.database.Database;
import dev.by1337.yaml.decoder.RecordYamlDecoder;
import dev.by1337.yaml.decoder.YamlDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Config {
    public static final YamlDecoder<Config> DECODER = RecordYamlDecoder.mapOf(
            Config::new,
            YamlDecoder.INT.fieldOf("port"),
            Database.DatabaseConfig.DECODER.fieldOf("database")
    );
    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private List<PublicKey> authorized_keys;
    public int tcp_port = 8013;
    public final Database.DatabaseConfig database_config;

    public Config(int tcp_port, Database.DatabaseConfig database_config) {
        this.tcp_port = tcp_port;
        this.database_config = database_config;
        reloadKeys();
    }

    public Config() {
        reloadKeys();
        database_config = null;
    }

    public void reloadKeys() {
        List<PublicKey> newKeys = new ArrayList<>();
        File home = new File("./authorized_keys");
        home.mkdirs();
        for (File file : home.listFiles()) {
            try {
                var data = Files.readString(file.toPath(), StandardCharsets.UTF_8);
                PublicKey key = Ed25519.publicKeyFromBase64(data);
                newKeys.add(key);
            } catch (Exception e) {
                log.error("Failed to load public key from {}", file.getPath());
            }
        }
        authorized_keys = Collections.unmodifiableList(newKeys);
    }

    public List<PublicKey> getAuthorizedKeys() {
        return authorized_keys;
    }
}
