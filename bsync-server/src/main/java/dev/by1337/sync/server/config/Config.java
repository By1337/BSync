package dev.by1337.sync.server.config;

import dev.by1337.sync.common.security.Ed25519;
import dev.by1337.sync.server.database.Database;
import dev.by1337.yaml.YamlMap;
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
    private PublicKey authorized_key;
    public int tcp_port = 8013;
    public final Database.DatabaseConfig database_config;

    public Config(int tcp_port, Database.DatabaseConfig database_config) {
        this.tcp_port = tcp_port;
        this.database_config = database_config;
        try {
            loadKey();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Config() {
        try {
            loadKey();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        database_config = null;
    }

    private void loadKey() throws Exception {
        File file = new File("./keys.yaml");
        if (file.exists()) {
            YamlMap map = YamlMap.load(file);
            authorized_key = Ed25519.publicKeyFromBase64(map.get("public_key").asString().result());

        } else {
            var pair = Ed25519.generateKeyPair();
            YamlMap map = new YamlMap();
            map.set("public_key", Ed25519.keyToBase64(pair.getPublic()));
            map.set("private_key", Ed25519.keyToBase64(pair.getPrivate()));
            Files.writeString(file.toPath(), map.saveToString(), StandardCharsets.UTF_8);
        }
    }

    public PublicKey getAuthorizedKey() {
        return authorized_key;
    }
}
