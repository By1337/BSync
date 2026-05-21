package dev.by1337.sync.common.work;

import java.util.Random;

public class EventLoopWorkers {
    private final EventLoopWorker[] workers;
    private final Random random = new Random();

    public EventLoopWorkers(String name, int count) {
        workers = new EventLoopWorker[count];
        for (int i = 0; i < count; i++) {
            workers[i] = new EventLoopWorker(String.format(name, i));
        }
    }

    public EventLoopWorker getRandom() {
        return workers[random.nextInt(workers.length)];
    }
}
