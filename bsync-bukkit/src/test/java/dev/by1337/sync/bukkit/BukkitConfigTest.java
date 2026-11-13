package dev.by1337.sync.bukkit;

import dev.by1337.sync.client.config.Config;
import dev.by1337.yaml.YamlValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BukkitConfigTest {
    private Map<String, Object> config() {
        return new HashMap<>(Map.of(
                "id", "client-1",
                "workers", 1,
                "netty_threads", -1,
                "servers", Map.of("main", Map.of(
                        "group", "player_data",
                        "ip", "localhost",
                        "port", 8013,
                        "private_key", "test-key"))));
    }

    @Test
    void existingConfigWithoutAliasesKeepsConnectionNames() {
        Config config = Decoders.CONFIG_DECODER.decode(YamlValue.wrap(config())).getOrThrow();

        assertEquals("main", config.resolveConnectionName("main"));
        assertEquals("unknown", config.resolveConnectionName("unknown"));
        assertEquals(1, config.servers.size());
        assertTrue(config.groupAliases.isEmpty());
        assertEquals(List.of("player_data"), config.resolveGroupNames("player_data"));
    }

    @Test
    void multipleAliasesResolveToTheSameServerWithoutDuplicatingConnections() {
        var yaml = config();
        yaml.put("aliases", Map.of("economy", "main", "players", "main"));
        Config config = Decoders.CONFIG_DECODER.decode(YamlValue.wrap(yaml)).getOrThrow();

        assertEquals("main", config.resolveConnectionName("economy"));
        assertEquals("main", config.resolveConnectionName("players"));
        assertEquals("main", config.resolveConnectionName("main"));
        assertEquals(1, config.servers.size());
        assertEquals("player_data", config.servers.get("main").group());
    }

    @Test
    void aliasesCannotShadowServerNames() {
        assertInvalidAliases(Map.of("main", "main"));
    }

    @Test
    void aliasesMustPointDirectlyToAnExistingServer() {
        assertInvalidAliases(Map.of("economy", "missing"));
        assertInvalidAliases(Map.of("economy", "players", "players", "main"));
        assertInvalidAliases(Map.of("economy", "economy"));
        assertInvalidAliases(Map.of("economy", "players", "players", "economy"));
    }

    @Test
    void blankAliasesAreRejected() {
        assertInvalidAliases(Map.of("", "main"));
        assertInvalidAliases(Map.of(" ", "main"));
    }

    @Test
    void originalSharedConfigConstructorRemainsCompatible() {
        Config decoded = Decoders.CONFIG_DECODER.decode(YamlValue.wrap(config())).getOrThrow();
        Config config = new Config(decoded.id, decoded.workers, decoded.netty_threads, decoded.servers);

        assertTrue(config.aliases.isEmpty());
        assertTrue(config.groupAliases.isEmpty());
        assertEquals("main", config.resolveConnectionName("main"));
        Config withAliases = new Config(decoded.id, decoded.workers, decoded.netty_threads, decoded.servers,
                Map.of("economy", "main"));
        assertEquals("main", withAliases.resolveConnectionName("economy"));
        assertTrue(withAliases.groupAliases.isEmpty());
    }

    @Test
    void groupAliasesCombineExistingGroupsAndHaveSeparateNamesFromConnectionAliases() {
        var yaml = config();
        var first = (Map<?, ?>) ((Map<?, ?>) yaml.get("servers")).get("main");
        var second = Map.of("group", "economy_data", "ip", "localhost", "port", 8013, "private_key", "test-key");
        yaml.put("servers", Map.of("main", first, "second", second));
        yaml.put("aliases", Map.of("all_data", "main"));
        yaml.put("group_aliases", Map.of("all_data", List.of("player_data", "economy_data")));
        Config config = Decoders.CONFIG_DECODER.decode(YamlValue.wrap(yaml)).getOrThrow();

        assertEquals(List.of("player_data", "economy_data"), config.resolveGroupNames("all_data"));
        assertEquals(List.of("player_data"), config.resolveGroupNames("player_data"));
        assertEquals(List.of("unknown"), config.resolveGroupNames("unknown"));
        assertEquals("main", config.resolveConnectionName("all_data"));
        assertEquals(2, config.servers.size());
    }

    @Test
    void invalidGroupAliasesAreRejected() {
        for (var aliases : List.of(
                Map.of("all_data", List.<String>of()),
                Map.of("all_data", List.of("missing")),
                Map.of("all_data", List.of("player_data", "missing")),
                Map.of("player_data", List.of("player_data")),
                Map.of("", List.of("player_data")),
                Map.of(" ", List.of("player_data")),
                Map.of("all_data", List.of("all_data")),
                Map.of("all_data", List.of("other"), "other", List.of("player_data")))) {
            var yaml = config();
            yaml.put("group_aliases", aliases);
            var result = Decoders.CONFIG_DECODER.decode(YamlValue.wrap(yaml));

            assertTrue(result.hasError(), aliases.toString());
            assertNull(result.result(), aliases.toString());
        }
    }

    private void assertInvalidAliases(Map<String, String> aliases) {
        var yaml = config();
        yaml.put("aliases", aliases);
        var result = Decoders.CONFIG_DECODER.decode(YamlValue.wrap(yaml));

        assertTrue(result.hasError());
        assertNull(result.result());
    }
}
