package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.util.SingleSemaphore;
import dev.by1337.sync.common.work.EventLoopWorker;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class Pipeline {
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    private Entry[] handlers = new Entry[0];
    private final EventLoopWorker eventLoop;
    private final MpscArrayQueue<PipelineTask> mailbox = new MpscArrayQueue<>(4096);
    private final SingleSemaphore draining = new SingleSemaphore();

    public Pipeline(EventLoopWorker eventLoop) {
        this.eventLoop = eventLoop;
    }


    public void schedule(ChannelMessage msg, Connection out, long ms) {
        eventLoop.schedule(() -> {
            handle0(msg, 0, out);
        }, ms);
    }
    public void handle(ChannelMessage msg, Connection out) {
        eventLoop.execute(() -> {
            handle0(msg, 0, out);
        });
       // if (!mailbox.offer(new PipelineTask(msg, out))) {
       //     log.error("Mailbox queue full! DROP TASK {}", msg, new Throwable());
       // } else {
       //     scheduleDrain();
       // }
    }

    public void registerAll(ChannelRuntime runtime, Runnable task) {
        eventLoop.execute(() -> {
            for (Entry handler : handlers) {
                handler.handler.init(runtime);
            }
            task.run();
        });
    }
    public CompletableFuture<Void> closeAll(){
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                for (Entry handler : handlers) {
                    handler.handler.close();
                }
            }finally {
                future.complete(null);
            }
        });
        return future;
    }

    private void scheduleDrain() {
        if (!draining.tryAcquire()) return;

        eventLoop.execute(() -> {
            try {
                drainMails();
            } finally {
                draining.release();

                if (!mailbox.isEmpty()) {
                    scheduleDrain();
                }
            }
        });
    }

    private void drainMails() {
        eventLoop.assertThread();
        PipelineTask msg;
        while ((msg = mailbox.poll()) != null) {
            try {
                handle0(msg.message(), 0, msg.connection);
            } catch (Exception e) {
                log.error("Failed to handle message {}", msg, e);
            }
        }
    }

    private void handle0(ChannelMessage msg, int idx, Connection connection) {
        if (idx >= handlers.length) {
            if (!(msg instanceof ChannelMessage.UnhandledIgnored))
                log.warn("unprocessed message {}", msg);
            return;
        }
        try (var ctx = new ChannelContextImpl(connection, idx, this)) {
            handlers[idx].handler.handle(ctx, msg);
        }
    }

    private static class ChannelContextImpl implements ChannelContext, AutoCloseable {
        private final Connection connection;
        private final int idx;
        private final Pipeline pipeline;
        private boolean closed;

        private ChannelContextImpl(Connection connection, int idx, Pipeline pipeline) {
            this.connection = connection;
            this.idx = idx;
            this.pipeline = pipeline;
        }

        @Override
        public void fire(ChannelMessage msg) {
            if (closed) throw new IllegalStateException("closed");
            pipeline.handle0(msg, idx + 1, connection);
        }

        @Override
        public Pipeline pipeline() {
            return pipeline;
        }

        @Override
        public Connection connection() {
            return connection;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    public Pipeline addLast(String name, ChannelHandler handler) {
        handlers = Arrays.copyOf(handlers, handlers.length + 1);
        handlers[handlers.length - 1] = new Entry(name, handler);
        return this;
    }

    public Pipeline addFirst(String name, ChannelHandler handler) {
        var arr = Arrays.copyOf(handlers, handlers.length + 1);
        System.arraycopy(handlers, 0, arr, 1, handlers.length);
        arr[0] = new Entry(name, handler);
        handlers = arr;
        return this;
    }

    public Entry[] getHandlers() {
        return Arrays.copyOf(handlers, handlers.length);
    }

    public record Entry(String name, ChannelHandler handler) {
    }

    record PipelineTask(
            ChannelMessage message,
            Connection connection
    ) {
    }
}
