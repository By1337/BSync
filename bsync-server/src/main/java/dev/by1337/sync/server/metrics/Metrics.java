package dev.by1337.sync.server.metrics;

import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class Metrics {
    public static final Metrics METRICS = new Metrics();

    private final Map<String, WindowedMetric> metrics =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService ticker;

    public Metrics() {
        this.ticker = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().factory()
        );

        ticker.scheduleAtFixedRate(
                this::tick,
                1,
                1,
                TimeUnit.SECONDS
        );
    }

    public WindowedMetric create(
            String name,
            MetricFormatter formatter,
            LongSupplier preTick
    ) {
        WindowedMetric metric = new WindowedMetric(
                formatter,
                preTick
        );

        metrics.put(name, metric);
        return metric;
    }

    public WindowedMetric get(String name) {
        return metrics.get(name);
    }

    private void tick() {
        for (WindowedMetric metric : metrics.values()) {
            metric.tick();
        }
    }

    public void dump(Logger logger) {
        for (Map.Entry<String, WindowedMetric> entry : metrics.entrySet()) {
            logger.info("{}\n{}", entry.getKey(), entry.getValue().snapshot().toString());
        }
    }
}