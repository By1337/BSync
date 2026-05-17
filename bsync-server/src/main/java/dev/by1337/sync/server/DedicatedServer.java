package dev.by1337.sync.server;

import dev.by1337.sync.server.config.Config;
import dev.by1337.sync.server.console.CommandManager;
import dev.by1337.sync.server.console.TerminalReader;
import dev.by1337.sync.server.network.ClientList;
import dev.by1337.sync.server.network.ConnectionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedicatedServer {
    private static final Logger log = LoggerFactory.getLogger(DedicatedServer.class);
    private final Config config;
    private final ConnectionListener connectionListener;
    private volatile boolean running;
    private final CommandManager commandManager;
    private final ClientList clientList;
    private final long millis;

    public DedicatedServer() {
        config = new Config();
        clientList = new ClientList();
        connectionListener = new ConnectionListener(this);
        running = true;
        log.info("Server started :{}", connectionListener.startTcpServerListener(config.tcp_port));
        commandManager = new CommandManager();
        millis = System.currentTimeMillis();
    }

    public long startMillis() {
        return millis;
    }

    public void readTerminal(){
        TerminalReader terminalReader = new TerminalReader(this, commandManager);
        terminalReader.start();
    }

    public boolean isRunning() {
        return running;
    }

    public void shutdown() {
        if (!running) return;
        running = false;
        connectionListener.stop();
    }

    public Config config() {
        return config;
    }

    public ConnectionListener connectionListener() {
        return connectionListener;
    }

    public CommandManager commandManager() {
        return commandManager;
    }
    public ClientList clientList() {
        return clientList;
    }
}
