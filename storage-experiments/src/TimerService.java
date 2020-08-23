import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

@FunctionalInterface
interface TimerTask {
    void execute();
}

public class TimerService {

    public class TaskId implements Comparable<TaskId> {

        private long ts;
        private final long seq;
        private final TimerTask task;
        private boolean repeat;
        private final long delay;

        public TaskId(long ts, long seq, TimerTask task, boolean repeat, long delay) {
            this.ts = ts;
            this.seq = seq;
            this.task = task;
            this.repeat = repeat;
            this.delay = delay;
        }

        @Override
        public int compareTo(TaskId o) {
            int cmp = Long.compare(ts, o.ts);
            if (cmp != 0) {
                return cmp;
            } else {
                return Long.compare(seq, o.seq);
            }
        }

        public TimerTask getTask() {
            return task;
        }

    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    TimerTask task = null;

                    synchronized (tasks) {
                        if (tasks.isEmpty()) {
                            tasks.wait();
                        } else {
                            TaskId id = tasks.first();
                            long curTs = System.currentTimeMillis();
                            if (id.ts > curTs) {
                                tasks.wait(id.ts - curTs);
                            } else {
                                tasks.pollFirst();
                                task = id.task;
                                if (id.repeat) {
                                    id.ts += id.delay;
                                    tasks.add(id);
                                }
                            }
                        }
                    }
                    if (task != null) {
                        task.execute();
                    }
                }
            } catch (InterruptedException e) {
                System.err.println("Worker is interrupted...");
            }
        }

    }

    private final TreeSet<TaskId> tasks = new TreeSet<>();
    private final AtomicLong taskSeq = new AtomicLong(0);

    private final Thread[] threads;

    public TimerService(int numThread) {
        this.threads = new Thread[numThread];
        for (int i = 0; i < numThread; i++) {
            threads[i] = new Thread(new Worker());
            threads[i].start();
        }
    }

    public TaskId execute(long delay, TimerTask task, boolean repeat) {
        TaskId id = new TaskId(System.currentTimeMillis() + delay, taskSeq.incrementAndGet(), task, repeat, delay);
        synchronized (tasks) {
            tasks.add(id);
            if (id == tasks.first()) {
                tasks.notify();
            }
        }
        return id;
    }

    public boolean cancel(TaskId id) {
        synchronized (tasks) {
            return tasks.remove(id);
        }
    }

    public static void main(String[] args) {
        TimerService service = new TimerService(4);
        TaskId id1 = service.execute(3000, () -> System.out.println("task 1 executed"), true);
        TaskId id2 = service.execute(5000, () -> System.out.println("task 2 executed"), true);
        service.cancel(id2);
    }

}