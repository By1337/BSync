package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.ChanneledPacket;
import dev.by1337.sync.common.packet.impl.RequestPacket;
import dev.by1337.sync.common.packet.impl.ResponsePacket;
import dev.by1337.sync.common.util.SingleSemaphore;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractChannel {
    private static final Logger log = LoggerFactory.getLogger(AbstractChannel.class);
    private final Connection connection;
    private final String id;
    private int requestId;
    private final Int2ObjectMap<RequestHolder> requests = new Int2ObjectOpenHashMap<>();
    private final SingleSemaphore requestsCleanupTask = new SingleSemaphore();

    protected AbstractChannel(Connection connection, String id) {
        this.connection = connection;
        this.id = id;
    }


    public void send(Packet packet) {
        connection.send(new ChanneledPacket(id, packet));
    }

    public void receive(Packet packet) {
        if (!connection.isWorkerThread()){
            connection.execute(() -> receive(packet));
            return;
        }
        if (packet instanceof RequestPacket r) {
            onRequest(r.payload, p -> send(new ResponsePacket(r.uid, p)));
        } else if (packet instanceof ResponsePacket r) {
            var v = requests.remove(r.uid);
            if (v != null) {
                try {
                    v.callback.accept(r.payload);
                } catch (Exception e) {
                    log.error("Failed to accept response {}", id, e);
                }
            } else {
                log.error("Got response for outdated request {} ", r);
            }
        }
    }

    public void request(Packet packet, PacketCallback consumer, long timeoutMs) {
        connection.execute(() -> {
            var id = requestId++;
            requests.put(id, new RequestHolder(System.currentTimeMillis() + timeoutMs, consumer));
            if (requestsCleanupTask.tryAcquire()) {
                connection.schedule(this::requestsTimeOuter, 250);
            }
            send(new RequestPacket(id, packet));
        });
    }

    private void requestsTimeOuter() {
        long now = System.currentTimeMillis();
        requests.int2ObjectEntrySet().removeIf(e -> {
            var v = e.getValue();
            if (v.timeoutAt <= now) {
                try {
                    v.callback.accept(null);
                } catch (Exception err) {
                    log.error("Failed to accept response {}", id, err);
                }
                return true;
            }
            return false;
        });
        if (requests.isEmpty()) {
            requestsCleanupTask.release();
        } else {
            connection.schedule(this::requestsTimeOuter, 250);
        }
    }

    protected abstract void onRequest(Packet packet, PacketCallback consumer);

    public abstract void onRegister();

    public abstract void onUnregister();

    public void onClose(){
        requests.values().forEach(v -> {
            try {
                v.callback.accept(null);
            }catch (Exception err){
                log.error("Failed to accept response {}", id, err);
            }
        });
        requests.clear();
    }

    public String id() {
        return id;
    }

    public static class RequestHolder implements Comparable<AbstractChannel.RequestHolder> {
        private final long timeoutAt;
        private final PacketCallback callback;

        public RequestHolder(long timeoutAt, PacketCallback callback) {
            this.timeoutAt = timeoutAt;
            this.callback = callback;
        }

        @Override
        public int compareTo(@NotNull AbstractChannel.RequestHolder o) {
            return Long.compare(timeoutAt, o.timeoutAt);
        }
    }
}
