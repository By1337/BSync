package dev.by1337.sync.common.transfer;

import dev.by1337.sync.common.work.EventLoopWorker;
import org.testng.annotations.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.IntStream;
import static org.testng.Assert.*;

public class BoundedParallelTest {
    private static final EventLoopWorker WORKER = new EventLoopWorker("parallel-test");

    @Test
    public void immediateResponsesFinishOnceWithoutRecursionOrPrematureCompletion() throws Exception {
        AtomicInteger results = new AtomicInteger();
        BoundedParallel.run(WORKER, IntStream.range(0, 50_000).boxed().iterator(), 256,
                value -> CompletableFuture.completedFuture(value), (value, response) -> {
                    assertEquals(value, response);
                    results.incrementAndGet();
                }).get(10, TimeUnit.SECONDS);
        assertEquals(results.get(), 50_000);
    }

    @Test
    public void rollsWindowForwardWithoutWaitingForSlowestRequestAndHonorsByteBudget() throws Exception {
        var requests = new LinkedBlockingQueue<CompletableFuture<Integer>>();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        var done = BoundedParallel.run(WORKER, List.of(0, 1, 2, 3, 4).iterator(), 4, 8,
                value -> 4, value -> null, value -> {
                    maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                    var future = new CompletableFuture<Integer>();
                    requests.add(future);
                    return future;
                }, (value, result) -> active.decrementAndGet());
        var slow = requests.poll(5, TimeUnit.SECONDS);
        var fast = requests.poll(5, TimeUnit.SECONDS);
        assertNotNull(slow);
        assertNotNull(fast);
        assertTrue(requests.isEmpty());
        fast.complete(1);
        var third = requests.poll(5, TimeUnit.SECONDS);
        assertNotNull(third, "A response must refill the window while the first request is still pending");
        third.complete(2);
        requests.poll(5, TimeUnit.SECONDS).complete(3);
        requests.poll(5, TimeUnit.SECONDS).complete(4);
        assertFalse(done.isDone());
        slow.complete(0);
        done.get(5, TimeUnit.SECONDS);
        assertEquals(maximum.get(), 2);
    }

    @Test
    public void serializesSameKeyAndPropagatesMakerFailureAfterInFlightRequestsSettle() throws Exception {
        var first = new CompletableFuture<Integer>();
        var entered = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        var done = BoundedParallel.run(WORKER, List.of(1, 1).iterator(), 2, 8, value -> 1, value -> value,
                value -> {
                    if (calls.incrementAndGet() == 1) {
                        entered.countDown();
                        return first;
                    }
                    throw new IllegalStateException("maker failed");
                }, (value, result) -> {
                });
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertEquals(calls.get(), 1);
        first.complete(1);
        assertThrows(ExecutionException.class, () -> done.get(5, TimeUnit.SECONDS));
    }
}
