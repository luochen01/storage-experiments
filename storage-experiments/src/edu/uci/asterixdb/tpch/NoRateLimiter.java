package edu.uci.asterixdb.tpch;

public class NoRateLimiter implements IRateLimiter {

    public static final NoRateLimiter INSTANCE = new NoRateLimiter();

    private NoRateLimiter() {
    }

    @Override
    public void add(int tokens) {

    }

    @Override
    public int get(int tokens) {
        return tokens;
    }

    @Override
    public void stop() {

    }
}
