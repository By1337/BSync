package dev.by1337.sync.common.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class SingleSemaphore {
    private final AtomicBoolean flag = new AtomicBoolean();

    public boolean tryAcquire() {
        return flag.compareAndSet(false, true);
    }

    public void release() {
        flag.set(false);
    }
}
