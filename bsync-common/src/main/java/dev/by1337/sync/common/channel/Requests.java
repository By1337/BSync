package dev.by1337.sync.common.channel;

import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.RequestPacket;
import dev.by1337.sync.common.packet.impl.ResponsePacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.function.Consumer;

public class Requests {
    private int requestId;
    private final Int2ObjectMap<RequestHolder> requests = new Int2ObjectOpenHashMap<>();
    private boolean requestsCleanupTask = false;
    private final EventLoopWorker worker;
    private final Logger log;

    public Requests(EventLoopWorker worker, Logger log) {
        this.worker = worker;
        this.log = log;
    }

    public void onResponse(ResponsePacket r){
        worker.assertThread();
        var v = requests.remove(r.uid);
        if (v != null) {
            try {
                v.callback.accept(r.payload);
            } catch (Exception e) {
                log.error("Failed to accept response", e);
            }
        } else {
            log.error("Got response for outdated request {} ", r);
        }
    }

    public void request(Packet packet, PacketCallback consumer, long timeoutMs, Consumer<Packet> out) {
        worker.execute(() -> {
            var id = requestId++;
            requests.put(id, new RequestHolder(System.currentTimeMillis() + timeoutMs, consumer));
            if (!requestsCleanupTask) {
                requestsCleanupTask = true;
                worker.schedule(this::requestsTimeOuter, 250);
            }
            out.accept(new RequestPacket(id, packet));
        });
    }

    private void requestsTimeOuter() {
        requestsCleanupTask = true;
        long now = System.currentTimeMillis();
        requests.int2ObjectEntrySet().removeIf(e -> {
            var v = e.getValue();
            if (v.timeoutAt <= now) {
                try {
                    v.callback.accept(null);
                } catch (Exception err) {
                    log.error("Failed to accept response", err);
                }
                return true;
            }
            return false;
        });
        if (requests.isEmpty()) {
            requestsCleanupTask = false;
        } else {
            worker.schedule(this::requestsTimeOuter, 250);
        }
    }
    public void close() {
        requests.values().forEach(v -> {
            try {
                v.callback.accept(null);
            } catch (Exception err) {
                log.error("Failed to accept empty response", err);
            }
        });
        requests.clear();
    }

    public static class RequestHolder implements Comparable<Requests.RequestHolder> {
        private final long timeoutAt;
        private final PacketCallback callback;

        public RequestHolder(long timeoutAt, PacketCallback callback) {
            this.timeoutAt = timeoutAt;
            this.callback = callback;
        }

        @Override
        public int compareTo(@NotNull Requests.RequestHolder o) {
            return Long.compare(timeoutAt, o.timeoutAt);
        }
    }
}
