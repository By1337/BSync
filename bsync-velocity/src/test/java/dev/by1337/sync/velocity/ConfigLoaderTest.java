package dev.by1337.sync.velocity;

import dev.by1337.sync.common.security.Ed25519;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.Map;
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
