package dev.by1337.sync.k2v.storage.sql;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class SqlStorageConfigTest {
    @Test
    void relativeSqliteFilesUsePluginDirectoryAndAbsoluteFilesKeepTheirLocation() {
        Path pluginDirectory = Path.of("plugins", "ExamplePlugin");
        var relative = SqlStorageConfig.parse("sqlite://players.db", pluginDirectory);
        assertEquals(pluginDirectory.resolve("players.db").toAbsolutePath().normalize().toString(), relative.database);
        var absolute = SqlStorageConfig.parse(relative.toString(), Path.of("another-plugin"));
        assertEquals(relative.database, absolute.database);
        assertEquals(relative.journalIdentity(), absolute.journalIdentity());
    }

    @Test
    void sqliteAcceptsRelativeAndAbsoluteFilesAndDefaultsToOneConnection() {
        var config = SqlStorageConfig.parse("sqlite://bsync/players+data.db?repository=rewards");
        assertTrue(config.isSqlite());
        assertEquals(Path.of("bsync/players+data.db").toAbsolutePath().normalize().toString(), config.database);
        assertEquals("rewards", config.repository);
        assertEquals(1, config.poolSize);
        assertEquals("org.sqlite.JDBC", config.driverClass());
        assertEquals("WAL", config.driverProperties().getProperty("journal_mode"));
        assertEquals("FULL", config.driverProperties().getProperty("synchronous"));
        assertEquals("1000", config.driverProperties().getProperty("busy_timeout"));
        assertEquals("IMMEDIATE", config.driverProperties().getProperty("transaction_mode"));
        var absolute = SqlStorageConfig.parse("sqlite://" + Path.of(config.database).toUri().getRawPath() + "?repository=rewards");
        assertEquals(config.jdbcUrl(), absolute.jdbcUrl());
        assertEquals(config.journalIdentity(), absolute.journalIdentity());
        assertEquals("players", SqlStorageConfig.parse("sqlite://players.db").repository);
        assertEquals(10_000, SqlStorageConfig.parse("sqlite://players.db?timeout=10s").timeoutMillis);
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("sqlite://dir/"));
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("sqlite://file.db?password=secret"));
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("sqlite://file.db?repository=../bad"));
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("sqlite://host:3306/file.db"));
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("sqlite://file.db?lease=30s"));
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("sqlite://file.db?renew=27s"));
    }

    @Test
    void parsesDefaultsAndDoesNotExposeCredentials() {
        var config = SqlStorageConfig.parse("mariadb://localhost/minecraft/players?user=plugin&password=very-secret");
        assertEquals(30_000, config.leaseMillis);
        assertEquals(27_000, config.renewMillis);
        assertEquals(4, config.poolSize);
        assertEquals("jdbc:mariadb://localhost:3306/minecraft", config.jdbcUrl());
        assertEquals("players", config.repository);
        assertEquals("very-secret", config.password);
        assertFalse(config.toString().contains("very-secret"));
        assertFalse(config.jdbcUrl().contains("very-secret"));
        assertEquals("false", config.driverProperties().getProperty("useAffectedRows"));
    }

    @Test
    void percentEncodedUserInfoPreservesLiteralPlus() {
        var config = SqlStorageConfig.parse("mysql://u%40x:p+a%3Ass@localhost:3307/minecraft/players?lease=40s&renew=37s&poll=10s&pool=6");
        assertEquals("u@x", config.user);
        assertEquals("p+a:ss", config.password);
        assertEquals(40_000, config.leaseMillis);
        assertEquals(37_000, config.renewMillis);
        assertEquals(6, config.poolSize);
    }

    @Test
    void rejectsUnsafeParametersAndRedactsParsingErrors() {
        for (String suffix : new String[]{ "&lease=0s", "&renew=30s", "&renew=29s", "&pool=0", "&lease=bad-secret",
                "&unknown=secret", "&user=duplicate", "&poll=0s", "&timeout=30s" }) {
            var error = assertThrows(IllegalArgumentException.class,
                    () -> SqlStorageConfig.parse("mysql://localhost/db/repo?user=plugin&password=actual-password" + suffix));
            assertFalse(error.toString().contains("actual-password"));
            assertNull(error.getCause());
        }
        assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("mysql://localhost/db?user=u"));
        var error = assertThrows(IllegalArgumentException.class, () -> SqlStorageConfig.parse("mysql://u:visible password@localhost/db/repo"));
        assertFalse(error.toString().contains("visible"));
        assertNull(error.getCause());
    }
}
