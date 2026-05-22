package dev.by1337.sync.server;

import dev.by1337.sync.common.work.EventLoopWorkers;
import dev.by1337.sync.server.channel.ChannelManager;
import dev.by1337.sync.server.config.Config;
import dev.by1337.sync.server.console.CommandManager;
import dev.by1337.sync.server.console.TerminalReader;
import dev.by1337.sync.server.network.ClientList;
import dev.by1337.sync.server.network.Connection;
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
    private final ChannelManager channelManager;
    private Thread terminalThread;

    public DedicatedServer() {
        config = new Config();
        clientList = new ClientList();
        connectionListener = new ConnectionListener(this);
        channelManager = new ChannelManager(new EventLoopWorkers("server-worker-%d", 4));
        running = true;
        log.info("Server started :{}", connectionListener.startTcpServerListener(config.tcp_port));
        commandManager = new CommandManager();
        millis = System.currentTimeMillis();
    }

    public long startMillis() {
        return millis;
    }

    public void readTerminal(){
        if (terminalThread != null) throw new IllegalStateException("Terminal thread has already been started");
        terminalThread = Thread.currentThread();
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
        terminalThread.interrupt();
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

    public ChannelManager channelManager() {
        return channelManager;
    }

    public void onConnect(Connection connection) {
        clientList.addConnection(connection);
    }

    public void onDisconnect(Connection connection) {
        clientList.removeConnection(connection);
        channelManager.onDisconnect(connection);
    }
}
