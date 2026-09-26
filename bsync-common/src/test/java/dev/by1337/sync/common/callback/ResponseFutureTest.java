package dev.by1337.sync.common.callback;

import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.*;

public class ResponseFutureTest {
    @Test
    public void notifiesCallbacksBeforeAndAfterCompletionOnce() {
        ResponseFuture<String> future = new ResponseFuture<>();
        List<String> results = new ArrayList<>();
        assertSame(future.then(results::add), future);
        assertFalse(future.hasResult());
        assertTrue(results.isEmpty());
        future.complete("value");
        future.then(results::add);
        assertTrue(future.hasResult());
        assertEquals(results, List.of("value", "value"));
        assertThrows(IllegalStateException.class, () -> future.complete("other"));
        assertEquals(results, List.of("value", "value"));
    }

    @Test
    public void chainsMappingAndWaitsForInnerFuture() {
        ResponseFuture<String> source = new ResponseFuture<>();
        ResponseFuture<Integer> inner = new ResponseFuture<>();
        ResponseFuture<Integer> mapped = source.map(String::length);
        ResponseFuture<Integer> flattened = source.flatMap(value -> inner);
        List<Integer> results = new ArrayList<>();
        mapped.then(results::add);
        flattened.then(results::add);
        source.complete("abc");
        assertEquals(results, List.of(3));
        assertFalse(flattened.hasResult());
        inner.complete(42);
        assertEquals(results, List.of(3, 42));
        assertTrue(flattened.hasResult());
    }

    @Test
    public void nullSkipsMappersAndTriggersFallbackAndEmptyCallback() {
        ResponseFuture<String> source = new ResponseFuture<>();
        AtomicInteger emptyCalls = new AtomicInteger();
        AtomicInteger mapperCalls = new AtomicInteger();
        source.ifEmpty(emptyCalls::incrementAndGet);
        source.ifPresent(value -> fail("Null must not invoke ifPresent"));
        ResponseFuture<Integer> mapped = source.map(value -> mapperCalls.incrementAndGet());
        ResponseFuture<Integer> flattened = source.flatMap(value -> {
            mapperCalls.incrementAndGet();
            return new ResponseFuture<>(1);
        });
        List<String> fallback = new ArrayList<>();
        source.orElse(() -> "fallback").then(fallback::add);
        source.complete(null);
        assertEquals(emptyCalls.get(), 1);
        assertEquals(mapperCalls.get(), 0);
        assertTrue(mapped.hasResult());
        assertTrue(flattened.hasResult());
        mapped.then(value -> assertNull(value));
        flattened.then(value -> assertNull(value));
        assertEquals(fallback, List.of("fallback"));
    }

    @Test
    public void presentResultDoesNotEvaluateFallback() {
        List<String> results = new ArrayList<>();
        new ResponseFuture<>("value").orElse(() -> {
            fail("Fallback must be lazy");
            return "unused";
        }).ifPresent(results::add).ifEmpty(() -> fail("Result is present"));
        assertEquals(results, List.of("value"));
    }

    @Test
    public void failingCallbackDoesNotPreventLaterCallbacks() {
        ResponseFuture<String> future = new ResponseFuture<>();
        List<String> results = new ArrayList<>();
        future.then(value -> { throw new IllegalArgumentException("test callback failure"); });
        future.then(results::add);
        future.complete("value");
        assertEquals(results, List.of("value"));
    }
}
