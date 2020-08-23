import java.util.concurrent.TimeUnit;

public class RateLimiter {
    private final int maxPermits;

    private long currentMicro;
    private double currentPermits;
    private final double permitsPerMicro;

    public RateLimiter(int tps, int maxPermits) {
        this.maxPermits = maxPermits;

        this.currentPermits = maxPermits;
        this.currentMicro = currentMicro();

        this.permitsPerMicro = tps / 1000.0 / 1000.0;
    }

    public void request() throws InterruptedException {
        synchronized (this) {
            while (currentPermits < 1.0) {
                refresh();
                if (currentPermits < 1.0) {
                    long sleepMicro = (long) ((1.0 - currentMicro) / permitsPerMicro);
                    sleep(sleepMicro);
                }
            }
            currentPermits -= 1.0;
        }

    }

    private void refresh() {
        synchronized (this) {
            long duration = currentMicro() - currentMicro;
            currentPermits = Math.min(maxPermits, currentPermits + duration * permitsPerMicro);
            currentMicro += duration;
        }
    }

    private long currentMicro() {
        long nano = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMicros(nano);
    }

    private void sleep(long micro) throws InterruptedException {
        this.wait(Math.max(1, TimeUnit.MICROSECONDS.toMillis(micro)));
    }

    public static void main(String[] args) throws InterruptedException {
        RateLimiter limiter = new RateLimiter(1, 10);

        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            limiter.request();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println("Thread " + Thread.currentThread().getName());
                    }
                }
            });
            threads[i].setName(String.valueOf(i));
            threads[i].start();
        }

        Thread.sleep(100000);

    }

}