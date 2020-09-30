package edu.uci.asterixdb.tpch;

public class RateLimiter implements IRateLimiter {

    private long count;

    private boolean stopped;

    @Override
    public synchronized void add(int tokens) {
        count += tokens;
        if (count == tokens) {
            this.notifyAll();
        }
    }

    @Override
    public synchronized int get(int tokens) {
        while (count == 0 && !stopped) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
        if (stopped) {
            return 0;
        }
        int consumed = (int) Math.min(count, tokens);
        count -= consumed;
        return consumed;
    }

    @Override
    public synchronized void stop() {
        stopped = true;
        this.notifyAll();
    }
}
