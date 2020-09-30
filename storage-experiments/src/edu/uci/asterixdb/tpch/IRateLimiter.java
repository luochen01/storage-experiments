package edu.uci.asterixdb.tpch;

public interface IRateLimiter {

    void add(int tokens);

    int get(int tokens);

    void stop();
}
