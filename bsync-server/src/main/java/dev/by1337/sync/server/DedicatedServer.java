package dev.by1337.sync.server;

import dev.by1337.sync.server.config.Config;
import dev.by1337.sync.server.console.CommandManager;
import dev.by1337.sync.server.console.TerminalReader;
import dev.by1337.sync.server.network.ClientList;
import dev.by1337.sync.server.network.ConnectionListener;

public class DedicatedServer {
    private final Config config;
    private final ConnectionListener connectionListener;
    private volatile boolean running;
    private final CommandManager commandManager;
    private final ClientList clientList;

    public DedicatedServer() {
        config = new Config();
        clientList = new ClientList();
        connectionListener = new ConnectionListener(this);
        running = true;
        connectionListener.startTcpServerListener(config.tcp_port);
        commandManager = new CommandManager();
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
