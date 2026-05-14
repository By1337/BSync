package dev.by1337.sync.client.network;

import dev.by1337.sync.client.config.ConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConnectionManager {
    private static final ScheduledExecutorService RECONNECT_SHEDULER = new ScheduledThreadPoolExecutor(1);
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);
    private final ConnectionConfig config;
    private final String id;
    private final ClientBootstrap bootstrap;
    private volatile boolean closed = false;
    private Connection connection;

    public ConnectionManager(ConnectionConfig config, String id, ClientBootstrap bootstrap) {
        this.config = config;
        this.id = id;
        this.bootstrap = bootstrap;
    }

    public void connect() {
        this.connection = new Connection(id, config, bootstrap, this);
        this.connection.connect();
    }

    void onClosed(Connection connection) {
        this.connection = null;
        if (closed) return;
        log.error("Connection closed {}:{}", config.ip, config.port);
        RECONNECT_SHEDULER.schedule(() -> {
            log.info("Reconnecting {}:{}", config.ip, config.port);
            this.connection = new Connection(id, config, bootstrap, this);
            this.connection.connect();
        }, 2, TimeUnit.SECONDS);
    }

    void postLogin(Connection connection) {
        log.info("Connected to {}:{}", config.ip, config.port);
    }

    public boolean hasConnection() {
        return connection != null && connection.connected();
    }

    public void close() {
        if (closed) return;
        closed = true;
        if (connection != null) {
            if (connection.connected()) {

            }
            connection.close();
        }
    }
}
