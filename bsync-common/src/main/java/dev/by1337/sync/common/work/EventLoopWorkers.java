package dev.by1337.sync.common.work;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class EventLoopWorkers {
    private final EventLoopWorker[] workers;
    private final AtomicInteger counter = new AtomicInteger(0);

    public EventLoopWorkers(String name, int count) {
        workers = new EventLoopWorker[count];
        for (int i = 0; i < count; i++) {
            workers[i] = new EventLoopWorker(String.format(name, i));
        }
    }

    public EventLoopWorker getNext() {
        return workers[counter.getAndIncrement() % workers.length];
    }

    public void forEach(Consumer<EventLoopWorker> c) {
        for (EventLoopWorker worker : workers) {
            c.accept(worker);
        }
    }
}
