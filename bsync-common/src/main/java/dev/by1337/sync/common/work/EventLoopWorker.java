package dev.by1337.sync.common.work;

import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public final class EventLoopWorker {
    private static final Logger log = LoggerFactory.getLogger(EventLoopWorker.class);
    private final MpscArrayQueue<Runnable> queue = new MpscArrayQueue<>(4096);
    private final PriorityQueue<ScheduledTask> scheduled = new PriorityQueue<>(256);
    private final Thread thread;
    private final String name;
    private final LongAdder busyNanos = new LongAdder();

    public EventLoopWorker(String name) {
        this.name = name;
        this.thread = new Thread(this::runLoop, name);
        this.thread.start();
    }

    public void execute(Runnable runnable) {
        if (isWorkerThread()){
            runnable.run();
            return;
        }
        if (!queue.offer(runnable)) {
            log.warn("Failed to add runnable to queue {}", runnable, new Throwable());
        }
        LockSupport.unpark(thread);
    }

    public void schedule(Runnable runnable) {
        schedule(runnable, 0);
    }
    public void schedule(Runnable runnable, long ms) {
        if (ms <= 0){
            if (!queue.offer(runnable)) {
                log.warn("Failed to add runnable to queue {}", runnable, new Throwable());
            }
            LockSupport.unpark(thread);
            return;
        }
        execute(() -> scheduled.add(new ScheduledTask(System.nanoTime() + (ms * 1_000_000), runnable)));
    }

    public int size() {
        return queue.size();
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
            runTask(task.task);
        }
    }

    private void runTask(Runnable r) {
        long start = System.nanoTime();
        try {
            r.run();
        } catch (Exception e) {
            log.error("Error while executing task", e);
        } finally {
            long time = System.nanoTime() - start;
            busyNanos.add(time);
            if (time > 10_000_000){
                log.warn("Task {} took {}ms",r, TimeUnit.NANOSECONDS.toMillis(time));
            }
        }
    }

    private void runTasks(Supplier<? extends @Nullable Runnable> queue) {
        Runnable task;
        while ((task = queue.get()) != null) {
            // System.out.println("run " + task);
            try {
                runTask(task);
            } catch (Throwable t) {
                log.error("Error while executing task", t);
            }
        }
    }

    public boolean isWorkerThread() {
        return thread == Thread.currentThread();
    }

    public void assertThread() {
        if (!isWorkerThread()) throw new IllegalStateException("Not a worker thread");
    }

    public String name() {
        return name;
    }

    public long busyNanosThenReset() {
        return busyNanos.sumThenReset();
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
        public int compareTo(@NotNull EventLoopWorker.ScheduledTask o) {
            return Long.compare(executeAt, o.executeAt);
        }
    }
}