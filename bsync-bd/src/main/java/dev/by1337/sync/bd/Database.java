package dev.by1337.sync.bd;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.by1337.yaml.decoder.RecordYamlDecoder;
import dev.by1337.yaml.decoder.YamlDecoder;

import java.util.concurrent.TimeUnit;

public class Database {

    private final HikariDataSource dataSource;
    private final String h2Folder;

    public Database(Database.DatabaseConfig cfg) {
        this(cfg, "./");
    }

    public Database(Database.DatabaseConfig cfg, String h2Folder) {
        dataSource = new HikariDataSource(createDbConfig(cfg));
        this.h2Folder = h2Folder.endsWith("/") ? h2Folder : h2Folder + "/";
    }

    public HikariDataSource dataSource() {
        return dataSource;
    }

    public void close() {
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

        if (cfg.type.equals("h2")) {
            hikariConfig.setJdbcUrl("jdbc:h2:file:" + h2Folder + "h2-" + cfg.database +
                    ";MODE=MySQL" +
                    ";DB_CLOSE_DELAY=-1" +
                    ";DATABASE_TO_UPPER=false");
            hikariConfig.setDriverClassName("org.h2.Driver");
        } else if (cfg.type.contains("mariadb")) {
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
