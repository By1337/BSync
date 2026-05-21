package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.channel.Requests;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.RequestPacket;
import dev.by1337.sync.common.packet.impl.ResponsePacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.network.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractChannel {
    protected final EventLoopWorker worker;
    protected final String id;
    protected final Logger log;
    private final Requests requests;

    public AbstractChannel(EventLoopWorker worker, String id) {
        this.worker = worker;
        this.id = id;
        log = LoggerFactory.getLogger(id + "|Channel");
        requests = new Requests(worker, log);
    }

    public void send(Packet packet, Connection connection) {
        connection.send(new ChanneledPacket(id, packet));
    }

    public final void receive(Packet packet, Connection connection) {
        worker.assertThread();
        if (packet instanceof RequestPacket r) {
            onRequest(r.payload, p -> send(new ResponsePacket(r.uid, p), connection), connection);
        } else if (packet instanceof ResponsePacket r) {
            requests.onResponse(r);
        } else {
            try {
                onReceive(packet, connection);
            } catch (Exception e) {
                log.error("Failed to handle packet {}:{}", packet, id, e);
            }
        }
    }
    public void request(Packet packet, PacketCallback consumer, long timeoutMs, Connection connection) {
        requests.request(packet, consumer, timeoutMs, connection::send);
    }
    public abstract void onConnect(Connection connection);
    public abstract void onDisconnect(Connection connection);

    protected abstract void onRequest(Packet packet, PacketCallback consumer, Connection connection);

    public abstract void onReceive(Packet packet, Connection connection);

}
