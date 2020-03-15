package edu.uci.asterixdb.storage.sim;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;

public class MaxMemoryQueue {
    private final LongArrayFIFOQueue lsnQueue = new LongArrayFIFOQueue();
    private final LongArrayFIFOQueue memoryQueue = new LongArrayFIFOQueue();

    public void add(long lsn, long memory) {

        while (!memoryQueue.isEmpty() && memoryQueue.lastLong() < memory) {
            lsnQueue.dequeueLastLong();
            memoryQueue.dequeueLastLong();
        }
        // add new
        lsnQueue.enqueue(lsn);
        memoryQueue.enqueue(memory);
    }

    public void truncate(long lsn) {
        while (!lsnQueue.isEmpty() && lsnQueue.firstLong() < lsn) {
            lsnQueue.dequeueLong();
            memoryQueue.dequeueLong();
        }
    }

    public long getMaxMemory() {
        return !memoryQueue.isEmpty() ? memoryQueue.firstLong() : 0;
    }

}
