package dev.by1337.sync.common.util;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Function;

public class BSUtils {
    private static final Logger log = LoggerFactory.getLogger(BSUtils.class);

    public static <T> FaultIsolation<T> faultIsolation(T v) {
        return new FaultIsolation<>(v);
    }

    public static boolean safe(ERunnable s) {
        try {
            s.run();
            return true;
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
            return false;
        }
    }

    @FunctionalInterface
    public interface ERunnable {
        void run() throws Exception;
    }

    public static <T> @Nullable T safe(ESupplier<T> s) {
        try {
            return s.get();
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
            return null;
        }
    }

    @FunctionalInterface
    public interface ESupplier<T> {
        T get() throws Exception;
    }


    public static class FaultIsolation<T> {
        private final T t;

        public FaultIsolation(T t) {
            this.t = t;
        }

        public boolean run(Consumer<T> c) {
            return safe(() -> c.accept(t));
        }

        @Nullable
        public <E> E get(Function<T, E> e) {
            return safe(() -> e.apply(t));
        }
    }

}
