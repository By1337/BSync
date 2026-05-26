package dev.by1337.sync.server;

import dev.by1337.sync.common.work.EventLoopWorkers;
import dev.by1337.sync.server.channel.ChannelManager;
import dev.by1337.sync.server.config.Config;
import dev.by1337.sync.server.console.CommandManager;
import dev.by1337.sync.server.console.TerminalReader;
import dev.by1337.sync.server.database.Database;
import dev.by1337.sync.server.network.ClientList;
import dev.by1337.sync.server.network.Connection;
import dev.by1337.sync.server.network.ConnectionListener;
import dev.by1337.yaml.YamlMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

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
    private final Database database;

    public DedicatedServer() {
        this(-1);
    }
    public DedicatedServer(int testPort) {
        config = Config.DECODER.decode(YamlMap.load(saveResourceToFile("config.yml")).get()).getOrThrow();
        if (testPort != -1){
            config.tcp_port = testPort;
        }
        database = new Database(config.database_config);
        clientList = new ClientList();
        connectionListener = new ConnectionListener(this);
        channelManager = new ChannelManager(new EventLoopWorkers("server-worker-%d", 4), this);
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
        channelManager.close();
        connectionListener.stop();
        if (terminalThread != null) {
            terminalThread.interrupt();
        }
        try {
            database.close();
        }catch (Exception e){
            log.error("Failed to close database connection!", e);
        }
        System.exit(1);
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

    public Database database() {
        return database;
    }

    private @Nullable InputStream getResource(@NotNull String filename) {
        try {
            URL url = DedicatedServer.class.getClassLoader().getResource(filename);
            if (url == null) {
                return null;
            } else {
                URLConnection connection = url.openConnection();
                connection.setUseCaches(false);
                return connection.getInputStream();
            }
        } catch (IOException var4) {
            return null;
        }
    }

    private File saveResourceToFile(String resource) {
        File outputFile = new File("./" + resource);
        if (outputFile.exists()) return outputFile;
        outputFile.getParentFile().mkdirs();

        try (var in = Objects.requireNonNull(getResource(resource), "Resource " + resource + " not found!")) {
            try (var out = new FileOutputStream(outputFile)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = in.read(buffer)) > 0) {
                    out.write(buffer, 0, length);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return outputFile;
    }
}
