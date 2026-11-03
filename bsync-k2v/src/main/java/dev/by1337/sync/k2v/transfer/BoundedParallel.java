package dev.by1337.sync.k2v.transfer;

import java.util.concurrent.Executor;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.*;

/** A rolling window; all iterator/counter access belongs to one worker. */
public final class BoundedParallel {
    private BoundedParallel() {
    }

    public static <T, R> CompletableFuture<Void> run(Executor worker, Iterator<T> iterator, int limit,
        Function<T, CompletableFuture<R>> maker, BiConsumer<T, R> then) {
        return run(worker, iterator, limit, Integer.MAX_VALUE, value -> 0, value -> null, maker, then);
    }

    public static <T, R> CompletableFuture<Void> run(Executor worker, Iterator<T> iterator, int limit,
        int byteLimit, ToIntFunction<T> weight, Function<T, ?> key,
        Function<T, CompletableFuture<R>> maker, BiConsumer<T, R> then) {
        if (limit < 1 || limit > 256 || byteLimit < 1) throw new IllegalArgumentException("Invalid parallel window");
        var result = new CompletableFuture<Void>();
        class Pump {
            int inflight;
            int bytes;
            Throwable failure;
            boolean exhausted;
            T waiting;
            boolean hasWaiting;
            final Set<Object> keys = new HashSet<>();

            void pump() {
                if (result.isDone()) return;
                try {
                    while (failure == null && !exhausted && inflight < limit) {
                        if (!hasWaiting) {
                            if (!iterator.hasNext()) {
                                exhausted = true;
                                break;
                            }
                            waiting = iterator.next();
                            hasWaiting = true;
                        }
                        T item = waiting;
                        int size = weight.applyAsInt(item);
                        if (size < 0) throw new IllegalArgumentException("Negative entry weight");
                        Object lane = key.apply(item);
                        if (inflight > 0 && ((long) bytes + size > byteLimit || (lane != null && keys.contains(lane)))) break;
                        hasWaiting = false;
                        waiting = null;
                        inflight++;
                        bytes += size;
                        if (lane != null) keys.add(lane);
                        try {
                            maker.apply(item).whenComplete((response, error) -> worker.execute(() -> {
                                inflight--;
                                bytes -= size;
                                if (lane != null) keys.remove(lane);
                                try {
                                    if (error != null) {
                                        if (failure == null) failure = error;
                                    } else then.accept(item, response);
                                } catch (Throwable e) {
                                    if (failure == null) failure = e;
                                }
                                pump();
                            }));
                        } catch (Throwable e) {
                            inflight--;
                            bytes -= size;
                            if (lane != null) keys.remove(lane);
                            failure = e;
                        }
                    }
                } catch (Throwable e) {
                    failure = e;
                }
                if (inflight == 0 && (exhausted || failure != null)) {
                    if (failure == null) result.complete(null);
                    else result.completeExceptionally(failure);
                }
            }
        }
        worker.execute(new Pump()::pump);
        return result;
    }
}
