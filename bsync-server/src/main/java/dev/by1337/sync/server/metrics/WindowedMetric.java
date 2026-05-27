package dev.by1337.sync.server.metrics;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public final class WindowedMetric {

    private static final int WINDOW_30S = 30;
    private static final int WINDOW_1M = 60;
    private static final int WINDOW_15M = 15 * 60;

    private final Window window30s;
    private final Window window1m;
    private final Window window15m;
    private final LongSupplier preTick;

    public WindowedMetric(MetricFormatter formatter, LongSupplier preTick) {
        window30s = new Window(WINDOW_30S, formatter);
        window1m = new Window(WINDOW_1M, formatter);
        window15m = new Window(WINDOW_15M, formatter);
        this.preTick = preTick;
    }

    public void record(long value) {
        window30s.record(value);
        window1m.record(value);
        window15m.record(value);
    }

    void tick() {
        record(preTick.getAsLong());
        window30s.tick();
        window1m.tick();
        window15m.tick();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                window30s.snapshot(),
                window1m.snapshot(),
                window15m.snapshot()
        );
    }

    private static final class Window {

        private static final int SAMPLE_SIZE = 2048;

        private final Bucket[] buckets;
        private final int size;

        private int currentIndex;
        private long lastTickSecond;

        private final long[] samples = new long[SAMPLE_SIZE];
        private final AtomicInteger sampleIndex = new AtomicInteger();
        private final  MetricFormatter formatter;

        private Window(int size, MetricFormatter formatter) {
            this.size = size;
            this.buckets = new Bucket[size];
            this.formatter = formatter;

            for (int i = 0; i < size; i++) {
                buckets[i] = new Bucket();
            }

            this.lastTickSecond = now();
        }

        void record(long value) {
            Bucket bucket = buckets[currentIndex];

            bucket.sum.add(value);
            bucket.count.increment();
            bucket.max.accumulate(value);

            int idx = sampleIndex.getAndIncrement();
            samples[idx & (SAMPLE_SIZE - 1)] = value;
        }

        void tick() {
            long now = now();
            long elapsed = now - lastTickSecond;

            if (elapsed <= 0) {
                return;
            }

            if (elapsed > size) {
                elapsed = size;
            }

            for (int i = 0; i < elapsed; i++) {
                currentIndex++;

                if (currentIndex >= size) {
                    currentIndex = 0;
                }

                buckets[currentIndex].reset();
            }

            lastTickSecond = now;
        }

        WindowSnapshot snapshot() {

            long totalSum = 0;
            long totalCount = 0;
            long max = 0;

            for (Bucket bucket : buckets) {
                totalSum += bucket.sum.sum();

                long count = bucket.count.sum();
                totalCount += count;

                long bucketMax = bucket.max.sum();
                if (bucketMax > max) {
                    max = bucketMax;
                }
            }

            double avg = totalCount == 0
                    ? 0
                    : (double) totalSum / totalCount;

            double p95 = percentile(0.95);

            return new WindowSnapshot(
                    avg,
                    max,
                    p95,
                    totalCount,
                    formatter
            );
        }

        private double percentile(double percentile) {

            int written = Math.min(
                    sampleIndex.get(),
                    SAMPLE_SIZE
            );

            if (written == 0) {
                return 0;
            }

            long[] copy = Arrays.copyOf(samples, written);
            Arrays.sort(copy);

            int index = (int) Math.ceil(percentile * written) - 1;

            if (index < 0) {
                index = 0;
            }

            if (index >= written) {
                index = written - 1;
            }

            return copy[index];
        }

        private static long now() {
            return System.currentTimeMillis() / 1000L;
        }
    }

    private static final class Bucket {

        private final LongAdder sum = new LongAdder();
        private final LongAdder count = new LongAdder();
        private final MaxAdder max = new MaxAdder();

        void reset() {
            sum.reset();
            count.reset();
            max.reset();
        }
    }

    private static final class MaxAdder {

        private volatile long value;

        void accumulate(long candidate) {
            if (candidate > value) {
                value = candidate;
            }
        }

        long sum() {
            return value;
        }

        void reset() {
            value = 0;
        }
    }

    public record WindowSnapshot(
            double avg,
            long max,
            double p95,
            long count,
            MetricFormatter formatter
    ) {

        public String simple() {
            return String.format(
                    "avg=%s max=%s p95=%s",
                    formatter.format(avg),
                    formatter.format(max),
                    formatter.format(p95)
            );
        }
    }

    public record Snapshot(
            WindowSnapshot s30,
            WindowSnapshot m1,
            WindowSnapshot m15
    ) {

        @Override
        public String toString() {
            return """
                    30s  -> %s
                    1m   -> %s
                    15m  -> %s
                    """.formatted(
                    s30.simple(),
                    m1.simple(),
                    m15.simple()
            );
        }
    }

    private static double nanosToMs(double nanos) {
        return nanos / 1_000_000.0;
    }
}