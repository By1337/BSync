package dev.by1337.sync.common.channel.handler;

import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelContext;
import dev.by1337.sync.common.channel.pipeline.ChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.impl.RequestPacket;
import dev.by1337.sync.common.packet.impl.ResponsePacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.PriorityQueue;

public class RequestsHandler implements ChannelHandler {
    private Logger log = DEFAULT_LOGGER;
    private final Int2ObjectMap<RequestHolder> requests = new Int2ObjectOpenHashMap<>();
    private final PriorityQueue<RequestHolder> requestsQueue = new PriorityQueue<>(256);
    private int requestId;
    private EventLoopWorker eventLoop;
    private boolean requestsCleanupTask = false;

    @Override
    public void init(ChannelRuntime runtime) {
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof RequestMsg<?> r) {
            ctx.connection().write(new RequestPacket(newRequest(r.consumer(), r.timeoutMs()).id, r.packet()));
            requestsTimeOuter();
        } else if (msg instanceof ResponsePacket r) {
            var v = requests.remove(r.uid());
            if (v != null) {
                // noinspection unchecked
                PacketCallback<Packet> callback = (PacketCallback<Packet>) v.callback;
                try {
                    callback.accept(r.payload(), ctx.connection());
                } catch (ClassCastException e) {
                    log.error("Bad response type", e);
                    try {
                        callback.accept(null, ctx.connection());
                    } catch (Exception e1) {
                        log.error("Failed to accept response", e1);
                    }
                } catch (Exception e) {
                    log.error("Failed to accept response", e);
                }
            }
        } else if (msg instanceof RequestPacket r) {
            ctx.pipeline().execute(
                    new IncomingRequest(r.payload(),
                            result -> ctx.connection().write(new ResponsePacket(r.uid(), result))),
                    ctx.connection()
            );
        } else {
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
        requests.values().forEach(v -> {
            try {
                v.callback.accept(null, null);
            } catch (Exception e) {
                log.error("Failed to accept response", e);
            }
        });
        requestsQueue.clear();
        requests.clear();
    }

    private RequestHolder newRequest(PacketCallback<?> consumer, long timeoutMs) {
        var id = requestId++;
        var v = new RequestHolder(id, System.currentTimeMillis() + timeoutMs, consumer);
        requests.put(id, v);
        requestsQueue.offer(v);
        return v;
    }

    public static <E extends Packet, T extends ExpectsResponse<E> & Packet> RequestMsg<E> request(T packet, PacketCallback<E> consumer, long timeoutMs) {
        return new RequestMsg<>(packet, consumer, timeoutMs);
    }

    private void requestsTimeOuter() {
        if (requestsCleanupTask) return;
        requestsCleanupTask = true;
        long now = System.currentTimeMillis();
        while (true) {
            var request = requestsQueue.peek();
            if (request == null || request.timeoutAt > now) break;
            requestsQueue.poll();
            if (requests.remove(request.id) != null) {
                try {
                    request.callback.accept(null, null);
                } catch (Exception err) {
                    log.error("Failed to accept response", err);
                }
            }
        }
        if (!requestsQueue.isEmpty()) {
            long delay = Math.max(1, requestsQueue.peek().timeoutAt - now);
            eventLoop.schedule(this::requestsTimeOuter, Math.min(250, delay));
        } else {
            requestsCleanupTask = false;
        }
    }

    public static class RequestHolder implements Comparable<RequestHolder> {
        private final int id;
        private final long timeoutAt;
        private final PacketCallback<?> callback;

        public RequestHolder(int id, long timeoutAt, PacketCallback<?> callback) {
            this.id = id;
            this.timeoutAt = timeoutAt;
            this.callback = callback;
        }

        @Override
        public int compareTo(@NotNull RequestHolder o) {
            return Long.compare(timeoutAt, o.timeoutAt);
        }
    }
}
