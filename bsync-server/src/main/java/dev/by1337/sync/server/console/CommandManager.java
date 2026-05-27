package dev.by1337.sync.server.console;

import dev.by1337.cmd.Command;
import dev.by1337.sync.server.DedicatedServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandManager {
    private static final Logger log = LoggerFactory.getLogger(CommandManager.class);
    private final Command<DedicatedServer> root;

    public CommandManager() {
        root = new Command<>("root");
        root.sub(new Command<DedicatedServer>("stop").executor(DedicatedServer::shutdown));
        root.sub(new Command<DedicatedServer>("uptime").executor(server -> {
            long uptime = System.currentTimeMillis() - server.startMillis();

            long seconds = (uptime / 1000) % 60;
            long minutes = (uptime / (1000 * 60)) % 60;
            long hours = (uptime / (1000 * 60 * 60)) % 24;
            long days = uptime / (1000 * 60 * 60 * 24);

            log.info("Server Uptime: {}d:{}h:{}m:{}s", days, hours, minutes, seconds);
        }));
    }

    public Command<DedicatedServer> getRootNode() {
        return root;
    }
}