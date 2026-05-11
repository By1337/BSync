package dev.by1337.sync.server.util;

import java.util.function.Supplier;

public class LazyLoad<T> implements Supplier<T> {
    private final Supplier<T> supplier;
    private T value;

    public LazyLoad(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        return value == null ? value = supplier.get() : value;
    }
    public boolean has(){
        return value != null;
    }
}
