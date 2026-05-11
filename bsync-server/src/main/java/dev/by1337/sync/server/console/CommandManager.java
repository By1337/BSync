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
        root.sub(new Command<DedicatedServer>("reload_keys").executor(server -> {
            server.config().reloadKeys();
            log.info("reload keys successful. keys {}", server.config().getAuthorizedKeys().size());
        }));
        root.sub(new Command<DedicatedServer>("stop").executor(DedicatedServer::shutdown));
    }

    public Command<DedicatedServer> getRootNode() {
        return root;
    }
}
