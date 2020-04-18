package edu.uci.asterixdb.storage.sim.lsm;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;

public class FlushLSNQueue {
    private final LongArrayFIFOQueue lsnQueue = new LongArrayFIFOQueue();
    private final LongArrayFIFOQueue memoryQueue = new LongArrayFIFOQueue();
    private long totalFlushedMemory = 0;

    public void flush(long lsn, long memory) {
        lsnQueue.enqueue(lsn);
        memoryQueue.enqueue(memory);
        totalFlushedMemory += memory;
    }

    public void truncate(long lsn) {
        while (!lsnQueue.isEmpty() && lsnQueue.firstLong() < lsn) {
            lsnQueue.dequeueLong();
            totalFlushedMemory -= memoryQueue.dequeueLong();
            assert totalFlushedMemory >= 0;
        }
    }

    public long getFlushedMemory() {
        return totalFlushedMemory;
    }

}
