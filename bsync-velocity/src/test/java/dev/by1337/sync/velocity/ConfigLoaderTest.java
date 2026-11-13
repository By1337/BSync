package dev.by1337.sync.velocity;

import dev.by1337.sync.common.security.Ed25519;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.Map;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {
    @TempDir
    Path directory;

    private String config(String key) {
        return "id: '{SERVER_ID}'\nworkers: 2\nnetty_threads: 1\nservers:\n  main:\n    group: network\n    ip: '{BSYNC_HOST}'\n    port: 8013\n    private_key: '" + key + "'\n";
    }

    @Test
    void expandsDockerIdentityAndConnectionValuesAndDecodesTypedConfig() throws Exception {
        var key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        var env = Map.of("SERVER_ID", "proxy-de-2", "BSYNC_HOST", "10.0.0.5");
        var value = ConfigLoader.decode(config(key), env::get);
        assertEquals("proxy-de-2", value.id);
        assertEquals(2, value.workers);
        assertEquals("10.0.0.5", value.servers.get("main").ip());
        assertEquals("network", value.servers.get("main").group());
        assertTrue(value.aliases.isEmpty());
        assertTrue(value.groupAliases.isEmpty());
        assertEquals(List.of("network"), value.resolveGroupNames("network"));
        assertEquals("main", value.resolveConnectionName("main"));
    }

    @Test
    void aliasesResolveWithoutAddingServers() throws Exception {
        var key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        var env = Map.of("SERVER_ID", "proxy-1", "BSYNC_HOST", "localhost");
        var value = ConfigLoader.decode(config(key) + "aliases:\n  economy: main\n  players: main\n", env::get);

        assertEquals("main", value.resolveConnectionName("economy"));
        assertEquals("main", value.resolveConnectionName("players"));
        assertEquals("unknown", value.resolveConnectionName("unknown"));
        assertEquals(1, value.servers.size());
    }

    @Test
    void invalidAliasesFailDuringConfigurationLoading() throws Exception {
        var key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        var env = Map.of("SERVER_ID", "proxy-1", "BSYNC_HOST", "localhost");
        for (String aliases : new String[]{
                "  main: main\n",
                "  economy: missing\n",
                "  economy: players\n  players: main\n",
                "  economy: economy\n",
                "  economy: players\n  players: economy\n",
                "  ' ': main\n"
        }) {
            assertThrows(IllegalArgumentException.class,
                    () -> ConfigLoader.decode(config(key) + "aliases:\n" + aliases, env::get));
        }
    }

    @Test
    void groupAliasesCombineGroupsWithoutCreatingServers() throws Exception {
        var key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        var env = Map.of("SERVER_ID", "proxy-1", "BSYNC_HOST", "localhost");
        var yaml = config(key) + "  secondary:\n    group: economy\n    ip: localhost\n    port: 8014\n    private_key: '" + key + "'\n"
                + "group_aliases:\n  all_data: [network, economy, network]\n";
        var value = ConfigLoader.decode(yaml, env::get);

        assertEquals(List.of("network", "economy", "network"), value.resolveGroupNames("all_data"));
        assertEquals(List.of("network"), value.resolveGroupNames("network"));
        assertEquals(2, value.servers.size());
    }

    @Test
    void invalidGroupAliasesFailDuringConfigurationLoading() throws Exception {
        var key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        var env = Map.of("SERVER_ID", "proxy-1", "BSYNC_HOST", "localhost");
        for (String aliases : new String[]{
                "  all_data: []\n",
                "  all_data: [missing]\n",
                "  all_data: [network, missing]\n",
                "  network: [network]\n",
                "  ' ': [network]\n",
                "  all_data: [all_data]\n",
                "  all_data: [other]\n  other: [network]\n"
        }) {
            assertThrows(IllegalArgumentException.class,
                    () -> ConfigLoader.decode(config(key) + "group_aliases:\n" + aliases, env::get));
        }
    }

    @Test
    void literalReplacementHandlesDollarBackslashAndPreservesUnknownTokens() {
        assertEquals("a$1\\b {UNKNOWN}", ConfigLoader.expand("{VALUE} {UNKNOWN}", name -> name.equals("VALUE") ? "a$1\\b" : null));
    }

    @Test
    void missingIdentityAndInvalidCredentialsAreRejectedWithoutLeakingSecrets() {
        assertThrows(IllegalArgumentException.class, () -> ConfigLoader.decode(config("secret-invalid-key"), key -> null));
        var error = assertThrows(IllegalArgumentException.class,
                () -> ConfigLoader.decode(config("secret-invalid-key"), key -> key.equals("SERVER_ID") ? "proxy-1" : "localhost"));
        assertFalse(error.toString().contains("secret-invalid-key"));
        assertNull(error.getCause());
    }

    @Test
    void invalidYamlAndWorkerCountsFailBeforeConnectionsAreCreated() throws Exception {
        var key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        assertThrows(IllegalArgumentException.class, () -> ConfigLoader.decode(config(key).replace("workers: 2", "workers: 0"), name -> "proxy-1"));
        assertThrows(IllegalArgumentException.class, () -> ConfigLoader.decode("id: [secret-bad-yaml", name -> null));
    }

    @Test
    void existingConfigurationIsNeverOverwritten() throws Exception {
        Path file = directory.resolve("config.yml");
        String key = Ed25519.keyToBase64(Ed25519.generateKeyPair().getPrivate());
        String content = config(key).replace("{SERVER_ID}", "literal-proxy").replace("{BSYNC_HOST}", "localhost");
        Files.writeString(file, content);
        assertEquals("literal-proxy", ConfigLoader.load(directory).id);
        assertEquals(content, Files.readString(file));
    }
}
