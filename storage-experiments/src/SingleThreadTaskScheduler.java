import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

interface Future<T> {
    T getResult() throws InterruptedException;

    boolean isDone();

    void cancel();

}

interface Task<T> {
    T execute();

    int priority();
}

interface TaskScheduler {
    <T> Future<T> schedule(Task<T> task);

    void start();

    void stop() throws InterruptedException;
}

public class SingleThreadTaskScheduler implements TaskScheduler {

    private class TaskWrapper<T> implements Future<T> {
        final Task<T> task;
        final long ts;

        volatile boolean isDone;
        volatile T result;

        public TaskWrapper(Task<T> task, long ts) {
            this.task = task;
            this.ts = ts;
        }

        @Override
        public T getResult() throws InterruptedException {
            synchronized (this) {
                if (!isDone) {
                    this.wait();
                }
            }
            return result;
        }

        public void doExecute() {
            T result = task.execute();
            synchronized (this) {
                isDone = true;
                this.result = result;
                this.notifyAll();
            }
        }

        @Override
        public boolean isDone() {
            return isDone;
        }

        @Override
        public void cancel() {
            synchronized (queue) {
                queue.remove(this);
            }
        }
    }

    private final TreeSet<TaskWrapper> queue = new TreeSet<>((t1, t2) -> {
        int cmp = Integer.compare(t1.task.priority(), t2.task.priority());
        if (cmp != 0) {
            return cmp;
        } else {
            return Long.compare(t1.ts, t2.ts);
        }
    });

    private final AtomicLong tsCounter = new AtomicLong(0);

    private volatile boolean stopped = false;

    private final Thread worker = new Thread(new Runnable() {
        @Override
        public void run() {
            try {
                while (!stopped) {
                    TaskWrapper<?> task = null;
                    while (task == null && !stopped) {
                        synchronized (queue) {
                            task = queue.pollFirst();
                            if (task == null) {
                                queue.wait();
                            }
                        }
                    }
                    if (stopped) {
                        break;
                    } else {
                        task.doExecute();
                    }
                }
            } catch (InterruptedException e) {
                System.err.println("Worker thread is interrupted " + e.getMessage());
            }
        }
    });

    @Override
    public void start() {
        worker.start();
    }

    @Override
    public void stop() throws InterruptedException {
        stopped = true;
        synchronized (queue) {
            queue.notifyAll();
        }
        worker.join();
    }

    @Override
    public <T> Future<T> schedule(Task<T> task) {
        TaskWrapper<T> wrapper = new TaskWrapper<>(task, tsCounter.incrementAndGet());
        enqueue(wrapper);
        return wrapper;
    }

    private <T> void enqueue(TaskWrapper<T> wrapper) {
        synchronized (queue) {
            queue.add(wrapper);
            if (queue.size() == 1) {
                queue.notifyAll();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        SingleThreadTaskScheduler scheduler = new SingleThreadTaskScheduler();

        scheduler.start();
        Future<Integer> future = scheduler.schedule(new Task<Integer>() {
            @Override
            public Integer execute() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return 1;
            }

            @Override
            public int priority() {
                return 0;
            }
        });
        System.out.println(future.getResult());

        scheduler.stop();
    }

}