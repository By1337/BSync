package dev.by1337.sync.velocity;

import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.common.security.Ed25519;
import dev.by1337.yaml.YamlMap;
import dev.by1337.yaml.decoder.RecordYamlDecoder;
import dev.by1337.yaml.decoder.YamlDecoder;
import dev.by1337.yaml.util.YamlReader;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

final class ConfigLoader {
    private static final Pattern ENV = Pattern.compile("\\{([A-Za-z_][A-Za-z0-9_]*)}");
    private static final YamlDecoder<ConnectionConfig> CONNECTION = RecordYamlDecoder.mapOf(ConnectionConfig::new,
            YamlDecoder.STRING.fieldOf("group"), YamlDecoder.STRING.fieldOf("ip"),
            YamlDecoder.INT.fieldOf("port"), YamlDecoder.STRING.fieldOf("private_key"));
    private static final YamlDecoder<Config> CONFIG = RecordYamlDecoder.mapOf(Config::new,
            YamlDecoder.STRING.fieldOf("id"), YamlDecoder.INT.fieldOf("workers", 1),
            YamlDecoder.INT.fieldOf("netty_threads", 1), YamlDecoder.mapOf(YamlDecoder.STRING, CONNECTION).fieldOf("servers"));

    static {
        // This module owns a relocated BYaml copy; it does not replace another plugin's YAML reader.
        YamlMap.setYamlReader(new YamlReader() {
            public YamlMap read(String data) {
                var options = new LoaderOptions();
                options.setAllowDuplicateKeys(false);
                java.util.LinkedHashMap<String, Object> root = new Yaml(new SafeConstructor(options)).load(data);
                return new YamlMap(root == null ? new java.util.LinkedHashMap<>() : root);
            }

            public String saveToString(Map<String, Object> data) {
                return new Yaml().dump(data);
            }
        });
    }

    static Config load(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path file = directory.resolve("config.yml");
        if (Files.notExists(file)) {
            try (var input = ConfigLoader.class.getResourceAsStream("/config.yml")) {
                if (input == null) throw new IOException("Bundled BSync config.yml is missing");
                Files.copy(input, file);
            }
        }
        return decode(Files.readString(file), System::getenv);
    }

    static Config decode(String yaml, Function<String, String> environment) {
        final Config config;
        try {
            config = CONFIG.decode(YamlMap.loadFromString(expand(yaml, environment)).get()).getOrThrow();
        } catch (RuntimeException error) {
            // Parser and decoder errors may include expanded credentials. Do not include their text/cause.
            throw new IllegalArgumentException("Invalid BSync config.yml; check required fields, YAML and environment variables");
        }
        if (config.id.isBlank() || ENV.matcher(config.id).find()) throw new IllegalArgumentException("Set a nonempty BSync id or SERVER_ID environment variable");
        if (config.workers < 1 || config.workers > 64) throw new IllegalArgumentException("BSync workers must be between 1 and 64");
        if (config.netty_threads != -1 && (config.netty_threads < 1 || config.netty_threads > 64))
            throw new IllegalArgumentException("BSync netty_threads must be -1 or between 1 and 64");
        if (config.servers.isEmpty()) throw new IllegalArgumentException("Configure at least one BSync connection");
        for (var entry : config.servers.entrySet()) {
            var value = entry.getValue();
            if (value.ip().isBlank() || ENV.matcher(value.ip()).find() || value.port() < 1 || value.port() > 65535)
                throw new IllegalArgumentException("Invalid BSync connection host/port");
            try {
                Ed25519.privateKeyFromBase64(value.private_key());
            } catch (Exception error) {
                throw new IllegalArgumentException("Invalid BSync private_key; configure a valid Ed25519 key");
            }
        }
        return config;
    }

    static String expand(String text, Function<String, String> environment) {
        return ENV.matcher(text).replaceAll(match -> {
            String value = environment.apply(match.group(1));
            return java.util.regex.Matcher.quoteReplacement(value == null ? match.group() : value);
        });
    }
}
