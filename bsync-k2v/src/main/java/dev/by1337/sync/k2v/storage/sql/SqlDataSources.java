package dev.by1337.sync.k2v.storage.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final class SqlDataSources {
    private SqlDataSources() {
    }

    static HikariDataSource create(SqlStorageConfig config, int size, String name) {
        try {
            Driver driver = (Driver) Class.forName(config.driverClass(), true, SqlDataSources.class.getClassLoader())
                    .getConstructor().newInstance();
            HikariConfig pool = new HikariConfig();
            pool.setPoolName(name);
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            try {
                // Provided Hikari may live in BLib's parent loader while the JDBC
                // driver lives in this plugin. Explicitly select both class and loader.
                thread.setContextClassLoader(SqlDataSources.class.getClassLoader());
                pool.setDriverClassName(config.driverClass());
            } finally {
                thread.setContextClassLoader(previous);
            }
            pool.setDataSource(new DriverSource(driver, config.jdbcUrl(), config.driverProperties(),
                    config.isSqlite() ? Path.of(config.database).getParent() : null));
            pool.setMaximumPoolSize(size);
            pool.setMinimumIdle(0);
            pool.setConnectionTimeout(config.timeoutMillis);
            pool.setValidationTimeout(Math.max(250, config.timeoutMillis));
            pool.setInitializationFailTimeout(-1);
            pool.setKeepaliveTime(0);
            pool.setIdleTimeout(60_000);
            return new HikariDataSource(pool);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("SQL JDBC driver is unavailable: " + config.driverClass(), e);
        }
    }

    /** Load JDBC drivers with the plugin's loader, independently of Hikari's provided classloader. */
    private record DriverSource(Driver driver, String url, Properties properties, Path directory) implements DataSource {
        @Override
        public String toString() {
            return "BSync JDBC datasource(" + url + ")";
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (directory != null) {
                try {
                    Files.createDirectories(directory);
                } catch (IOException e) {
                    throw new SQLException("Cannot create SQLite directory", e);
                }
            }
            Connection connection = driver.connect(url, properties);
            if (connection == null) throw new SQLException("JDBC driver rejected the SQL storage URL");
            return connection;
        }

        @Override
        public Connection getConnection(String user, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter writer) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public void setLoginTimeout(int timeout) {
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger("bsync-sql");
        }

        @Override
        public <T> T unwrap(Class<T> type) throws SQLException {
            if (type.isInstance(this)) return type.cast(this);
            throw new SQLException("Not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> type) {
            return type.isInstance(this);
        }
    }
}
