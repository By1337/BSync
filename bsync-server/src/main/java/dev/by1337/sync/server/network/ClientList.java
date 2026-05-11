package dev.by1337.sync.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientList {
    private static final Logger log = LoggerFactory.getLogger(ClientList.class);
    private final List<Connection> connections = new CopyOnWriteArrayList<>();

    public void addConnection(Connection connection) {
        if (connections.contains(connection)) return;
        connections.add(connection);
        log.info("Connected {}:[{}]", connection.id(), connection.channel().remoteAddress());
    }
    public void removeConnection(Connection connection) {
        if (connections.remove(connection)){
            log.info("Disconnected {}:[{}]", connection.id(), connection.channel().remoteAddress());
        }
    }

    public List<Connection> connections() {
        return Collections.unmodifiableList(connections);
    }
}
