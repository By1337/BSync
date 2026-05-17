package dev.by1337.sync.client.work;

import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public final class ConnectionWorker {
    private static final Logger log = LoggerFactory.getLogger(ConnectionWorker.class);
    private final MpscArrayQueue<Runnable> queue = new MpscArrayQueue<>(4096);
    private final PriorityQueue<ScheduledTask> scheduled = new PriorityQueue<>(256);
    private final Thread thread;

    public ConnectionWorker(String name) {
        this.thread = new Thread(this::runLoop, name);
        this.thread.start();
    }

    public void execute(Runnable runnable) {
        if (!queue.add(runnable)) {
            log.warn("Failed to add runnable to queue", new Throwable());
        }
        LockSupport.unpark(thread);
    }

    public void schedule(Runnable runnable, long ms) {
        execute(() -> scheduled.add(new ScheduledTask(System.nanoTime() + (ms * 1_000_000), runnable)));
    }

    private void runLoop() {
        while (true) {
            runScheduledTasks();
            runTasks(queue::poll);
            if (scheduled.isEmpty()) {
                LockSupport.park();
            } else {
                LockSupport.parkNanos(1_000_000);
            }
        }
    }

    private void runScheduledTasks() {
        long now = System.nanoTime();

        while (true) {
            ScheduledTask task = scheduled.peek();

            if (task == null || task.executeAt > now) {
                return;
            }

            scheduled.poll();

            try {
                task.task.run();
            } catch (Throwable t) {
                log.error("Error", t);
            }
        }
    }

    private void runTasks(Supplier<? extends @Nullable Runnable> queue) {
        Runnable task;
        while ((task = queue.get()) != null) {
            try {
                task.run();
            } catch (Throwable t) {
                log.error("Error while executing task", t);
            }
        }
    }
    public boolean isWorkerThread() {
        return thread == Thread.currentThread();
    }

    public static class ScheduledTask implements Runnable, Comparable<ScheduledTask> {
        private final long executeAt;
        private final Runnable task;

        public ScheduledTask(long executeAt, Runnable task) {
            this.executeAt = executeAt;
            this.task = task;
        }


        @Override
        public void run() {
            task.run();
        }

        @Override
        public int compareTo(@NotNull ConnectionWorker.ScheduledTask o) {
            return Long.compare(executeAt, o.executeAt);
        }
    }
}