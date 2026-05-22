package dev.by1337.sync.bukkit;

import dev.by1337.sync.client.config.Config;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.yaml.decoder.RecordYamlDecoder;
import dev.by1337.yaml.decoder.YamlDecoder;

import java.io.File;

class Decoders {
    public static final YamlDecoder<ConnectionConfig> CONNECTION_CONFIG_DECODER = RecordYamlDecoder.mapOf(
            ConnectionConfig::new,
            YamlDecoder.STRING.fieldOf("ip"),
            YamlDecoder.INT.fieldOf("port"),
            YamlDecoder.STRING.flatMap((ctx, s) -> ctx.get(File.class)
                            .map(base -> new File(base, "keys/"+s)))
                    .fieldOf("key")
    );

    public static final YamlDecoder<Config> CONFIG_DECODER = RecordYamlDecoder.mapOf(
            Config::new,
            YamlDecoder.STRING.fieldOf("id"),
            YamlDecoder.INT.fieldOf("workers"),
            YamlDecoder.INT.fieldOf("netty_threads"),
            YamlDecoder.mapOf(YamlDecoder.STRING, CONNECTION_CONFIG_DECODER)
                    .fieldOf("servers")
    );
}
