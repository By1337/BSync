package dev.by1337.sync.server.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.yaml.decoder.RecordYamlDecoder;
import dev.by1337.yaml.decoder.YamlDecoder;

import java.util.concurrent.TimeUnit;

public class Database {

    private final HikariDataSource dataSource;

    public Database(Database.DatabaseConfig cfg) {
        dataSource = new HikariDataSource(createDbConfig(cfg));
    }

    public HikariDataSource dataSource() {
        return dataSource;
    }

    public void close(){
        dataSource.close();
    }

    private HikariConfig createDbConfig(Database.DatabaseConfig cfg) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(cfg.user);
        hikariConfig.setPassword(cfg.password);
        hikariConfig.setMaximumPoolSize(3);
        hikariConfig.setKeepaliveTime(TimeUnit.MINUTES.toMillis(4));
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);

        if (cfg.type.contains("mariadb")) {
            hikariConfig.setJdbcUrl(
                    "jdbc:mariadb://" + cfg.host + ":" + cfg.port + "/" + cfg.database
            );
            hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        } else {
            hikariConfig.setJdbcUrl(
                    "jdbc:mysql://" + cfg.host + ":" + cfg.port + "/" + cfg.database
            );
        }

        return hikariConfig;
    }

    public static class DatabaseConfig {
        public static final YamlDecoder<DatabaseConfig> DECODER = RecordYamlDecoder.mapOf(
                DatabaseConfig::new,
                YamlDecoder.STRING.fieldOf("type", "disabled"),
                YamlDecoder.STRING.fieldOf("user", "root"),
                YamlDecoder.STRING.fieldOf("password", "password"),
                YamlDecoder.STRING.fieldOf("host", "localhost"),
                YamlDecoder.INT.fieldOf("port", 3306),
                YamlDecoder.STRING.fieldOf("database", "mydatabase")
        );
        public final String type;
        public final String user;
        public final String password;
        public final String host;
        public final int port;
        public final String database;

        public DatabaseConfig(String type, String user, String password, String host, int port, String database) {
            this.type = type;
            this.user = user;
            this.password = password;
            this.host = host;
            this.port = port;
            this.database = database;
        }
    }
}
