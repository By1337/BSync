package dev.by1337.sync.server.database.table;

import dev.by1337.sync.common.work.EventLoopWorker;
import io.netty.util.internal.shaded.org.jctools.queues.MpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DataBatcher<T> {
    private static final Logger log = LoggerFactory.getLogger(DataBatcher.class);
    private final MpmcArrayQueue<T> queue;
    private final Flusher<T> flusher;
    private final int load50;

    private volatile boolean closed;
    private final AtomicInteger size = new AtomicInteger();
    private final Runnable ioPing;

    private DataBatcher(final int capacity, Flusher<T> flusher, Runnable ioPing) {
        queue = new MpmcArrayQueue<>(capacity);
        load50 = capacity / 2;
        this.flusher = flusher;
        this.ioPing = ioPing;
    }

    public DataBatcher(final int capacity, Flusher<T> flusher, EventLoopWorker worker) {
        queue = new MpmcArrayQueue<>(capacity);
        load50 = capacity / 2;
        this.flusher = flusher;
        worker.repeat(this::ioTick, 100, () -> closed);

        AtomicBoolean load50Ping = new AtomicBoolean();
        ioPing = () -> {
            if (load50Ping.compareAndSet(false, true)) {
                worker.execute(() -> {
                    try {
                        ioTick();
                    } finally {
                        load50Ping.set(false);
                    }
                });
            }
        };
    }

    public static <T, R> BatcherPair<T, R> createPair(
            final int capacity,
            EventLoopWorker worker,
            Flusher<T> tFlusher,
            Flusher<R> rFlusher
    ) {
        var holder = new Object() {
            final AtomicBoolean load50Ping = new AtomicBoolean();
            final DataBatcher<T> tBatcher = new DataBatcher<>(capacity, tFlusher, this::ioPing);
            final DataBatcher<R> rBatcher = new DataBatcher<>(capacity, rFlusher, this::ioPing);

            public void ioPing() {
                if (load50Ping.compareAndSet(false, true)) {
                    worker.execute(() -> {
                        try {
                            if (!tBatcher.closed) tBatcher.ioTick();
                            if (!rBatcher.closed) rBatcher.ioTick();
                        } finally {
                            load50Ping.set(false);
                        }
                    });
                }
            }

            public void startFlusher() {
                worker.repeat(() -> {
                    if (!tBatcher.closed) tBatcher.ioTick();
                    if (!rBatcher.closed) rBatcher.ioTick();
                }, 100, () -> tBatcher.closed && rBatcher.closed);
            }
        };
        holder.startFlusher();
        return new BatcherPair<>(holder.tBatcher, holder.rBatcher);
    }

    public record BatcherPair<T, R>(DataBatcher<T> first, DataBatcher<R> second) {
    }

    private void ioTick() {
        if (closed) return;
        if (size.get() != 0)
            flush(Integer.MAX_VALUE);
    }

    public void offer(T t) {
        if (closed) throw new IllegalStateException("Batch is closed!");
        if (!queue.offer(t)) {
            log.warn("DataBatcher queue is full, forcing flush");

            flush(queue.capacity() / 4);

            if (!queue.offer(t)) {
                throw new IllegalStateException(
                        "Failed to enqueue value after forced flush"
                );
            }
            size.incrementAndGet();
        } else {
            if (size.incrementAndGet() >= load50) {
                ioPing.run();
            }
        }
    }

    private void flush(int limit) {
        try {
            flusher.accept(queue, limit, v -> size.decrementAndGet());
        } catch (SQLException e) {
            log.error("Failed to flush", e);
        }
    }

    public void close() {
        if (closed) return;
        closed = true;
        try {
            flusher.accept(queue, Integer.MAX_VALUE, v -> size.decrementAndGet());
        } catch (SQLException e) {
            log.error("Failed to flush", e);
        }
    }

    @FunctionalInterface
    public interface Flusher<T> {
        void accept(Queue<T> t, int limit, Consumer<T> c) throws SQLException;
    }
}
