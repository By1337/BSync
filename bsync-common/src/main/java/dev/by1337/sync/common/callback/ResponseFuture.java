package dev.by1337.sync.common.callback;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.packet.ExpectsResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.*;

public class ResponseFuture<T> {
    private static final Logger log = LoggerFactory.getLogger(ResponseFuture.class);
    private @Nullable T result;
    private volatile boolean hasResult;
    private final List<Consumer<@Nullable T>>  consumers = new ArrayList<>();

    public ResponseFuture() {
    }

    public ResponseFuture(@Nullable T result) {
        this.result = result;
        hasResult = true;
    }

    public void complete(@Nullable T res) {
        if (hasResult) {
            throw new IllegalStateException("Duplicated complete " + res);
        }
        hasResult = true;
        result = res;
        if (!consumers.isEmpty()) {
            for (var consumer : consumers) {
                try {
                    consumer.accept(result);
                } catch (Exception e) {
                    log.error("Failed to accept response", e);
                }
            }
            consumers.clear();
        }
    }

    public ResponseFuture<T> then(Consumer<@Nullable T> consumer) {
        if (hasResult) {
            consumer.accept(result);
        } else {
            consumers.add(consumer);
        }
        return this;
    }

    public ResponseFuture<T> ifEmpty(Runnable r) {
        return then(p -> {
            if (p == null) r.run();
        });
    }

    public ResponseFuture<T> ifPresent(Consumer<@NotNull T> consumer) {
        return then(p -> {
            if (p != null) consumer.accept(p);
        });
    }

    public ResponseFuture<T> orElse(Supplier<T> s) {
        ResponseFuture<T> r = new ResponseFuture<>();
        then(t -> {
            if (t == null) r.complete(s.get());
            else r.complete(t);
        });
        return r;
    }

    public <R> ResponseFuture<R> map(Function<@NotNull T, R> mapper) {
        ResponseFuture<R> r = new ResponseFuture<>();
        then((t) -> {
            if (t == null) r.complete(null);
            else r.complete(mapper.apply(t));
        });
        return r;
    }

    public <R> ResponseFuture<R> flatMap(Function<@NotNull T, ResponseFuture<R>> req) {
        ResponseFuture<R> r = new ResponseFuture<>();
        then((t) -> {
            if (t != null) {
                req.apply(t).then(r::complete);
            } else {
                r.complete(null);
            }
        });
        return r;
    }

    public boolean hasResult() {
        return hasResult;
    }
}
