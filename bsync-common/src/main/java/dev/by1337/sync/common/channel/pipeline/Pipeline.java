package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class Pipeline {
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    private Entry[] handlers = new Entry[0];
    private final EventLoopWorker eventLoop;

    public Pipeline(EventLoopWorker eventLoop) {
        this.eventLoop = eventLoop;
    }


    public void schedule(ChannelMessage msg, Connection out, long ms) {
        eventLoop.schedule(() -> {
            execute0(msg, 0, out);
        }, ms);
    }

    public void execute(ChannelMessage msg, Connection out) {
        eventLoop.execute(() -> {
            execute0(msg, 0, out);
        });
    }

    private void execute0(ChannelMessage msg, int idx, Connection connection) {
        if (idx >= handlers.length) {
            if (!(msg instanceof ChannelMessage.UnhandledIgnored))
                log.warn("unprocessed message {}", msg);
            return;
        }
        try (var ctx = new ChannelContextImpl(connection, idx, this)) {
            handlers[idx].handler.handle(ctx, msg);
        }
    }

    public void registerAll(ChannelRuntime runtime) {
        eventLoop.execute(() -> {
            for (Entry handler : handlers) {
                handler.handler.init(runtime);
            }
        });
    }

    public CompletableFuture<Void> closeAll() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                for (Entry handler : handlers) {
                    handler.handler.close();
                }
            } finally {
                //сами handler'ы могут ложить новые таски в eventLoop, отпустим future после тех тасков
                eventLoop.execute(() -> future.complete(null));
            }
        });
        return future;
    }

    public ChannelHandler getHandler(String name) {
        for (Entry handler : handlers) {
            if (handler.name.equals(name)) return handler.handler;
        }
        throw new IllegalArgumentException("Unknown handler " + name);
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
            pipeline.execute0(msg, idx + 1, connection);
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
