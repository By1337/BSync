package dev.by1337.sync.server.metrics;

@FunctionalInterface
public interface MetricFormatter {
    String format(double value);

    default String format(long value) {
        return format((double) value);
    }

    static MetricFormatter nanos() {
        return nanos -> "%.2fms".formatted(nanos / 1_000_000.0);
    }

    static MetricFormatter number() {
        return "%.2f"::formatted;
    }

    static MetricFormatter bytes() {
        return bytes -> {
            double value = bytes;
            String unit = "B";

            if (value >= 1024) {
                value /= 1024;
                unit = "KB";
            }

            if (value >= 1024) {
                value /= 1024;
                unit = "MB";
            }

            if (value >= 1024) {
                value /= 1024;
                unit = "GB";
            }

            return "%.2f%s".formatted(value, unit);
        };
    }
}
