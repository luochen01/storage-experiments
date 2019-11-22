package edu.uci.asterixdb.storage.sim;

import java.util.ArrayDeque;

import org.apache.commons.math3.stat.regression.SimpleRegression;

class Point {
    double memory;
    double derivative;

    public Point(double memory, double benefit) {
        this.memory = memory;
        this.derivative = benefit;
    }

    @Override
    public String toString() {
        return memory + "\t" + derivative;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Point) {
            Point p = (Point) obj;
            return Math.abs(memory - p.memory) < 1;
        } else {
            return false;
        }
    }
}

class MemoryTuner {
    public static final double MIN_PERCENT = 0.011;
    public static final double INITIAL_PERCENT = 0.05;
    public static final double MAX_PERCENT = 0.1;
    public static final double P = 1.0;
    public static final int HISTORY_SIZE = 3;
    private final LSMSimulator simulator;

    private int memoryComponentSize;
    private int bufferCacheSize;
    private ArrayDeque<Point> memoryComponentHistory = new ArrayDeque<>(HISTORY_SIZE);

    public MemoryTuner(LSMSimulator simulator) {
        this.simulator = simulator;

        this.memoryComponentSize = simulator.config.memConfig.totalMemSize;
        this.bufferCacheSize = simulator.config.tuningConfig.cacheSize;
    }

    // we only need to decide the memory allocated to the memory components
    public void tune() {
        double writes = simulator.writes - lastWrites;

        double writeDerivative = computeDiskWriteDerivative() * simulator.config.tuningConfig.writeWeight / writes
                * simulator.config.tuningConfig.tuningCycle;
        double readDerivative = computeDiskReadDerivative() * simulator.config.tuningConfig.readWeight / writes
                * simulator.config.tuningConfig.tuningCycle;

        double totalDerivative = writeDerivative + readDerivative;

        Point p = new Point(memoryComponentSize, totalDerivative);
        if (!memoryComponentHistory.contains(p)) {
            if (memoryComponentHistory.size() == HISTORY_SIZE) {
                memoryComponentHistory.removeFirst();
            }
            System.out.println(memoryComponentSize + "\t" + totalDerivative);
            memoryComponentHistory.addLast(new Point(memoryComponentSize, totalDerivative));
        }
        double delta = 0;
        if (memoryComponentHistory.size() > 1) {
            // ideally, totalDerivative should be 0
            SimpleRegression regression = buildRegressionModel();
            delta = Math.abs(P * totalDerivative / regression.getSlope());
        } else {
            delta = Math.abs(INITIAL_PERCENT * (memoryComponentSize + bufferCacheSize));
        }
        delta = Math.min(delta, (memoryComponentSize + bufferCacheSize) * MAX_PERCENT);

        long oldMemoryComponentSize = memoryComponentSize;
        long oldBufferCacheSize = bufferCacheSize;
        if (delta > (memoryComponentSize + bufferCacheSize) * MIN_PERCENT) {
            if (totalDerivative < 0) {
                delta = Math.min(delta, bufferCacheSize - simulator.config.tuningConfig.minMemorySize);
                // we can make more saving by allocating more memory
                bufferCacheSize -= delta;
                simulator.updateBufferCacheSize(bufferCacheSize);
                memoryComponentSize += delta;
                simulator.updateMemoryComponentSize(memoryComponentSize);
            } else {
                delta = Math.min(delta, memoryComponentSize - simulator.config.tuningConfig.minMemorySize);
                memoryComponentSize -= delta;
                simulator.updateMemoryComponentSize(memoryComponentSize);
                bufferCacheSize += delta;
                simulator.updateBufferCacheSize(bufferCacheSize);
            }
        }

        System.out.println(String.format(
                "Dwrite: %.2f, Dread: %.2f, Dtotal: %.2f, delta: %.2f, memory component size: %d->%d, buffer cache size: %d->%d",
                writeDerivative, readDerivative, totalDerivative, delta, oldMemoryComponentSize, memoryComponentSize,
                oldBufferCacheSize, bufferCacheSize));

        resetStats();
    }

    private static boolean sameSign(double a, double b) {
        return (a < 0 && b < 0) || (a > 0 && b > 0);
    }

    private SimpleRegression buildRegressionModel() {
        SimpleRegression regression = new SimpleRegression();

        for (Point p : memoryComponentHistory) {
            regression.addData(p.memory, p.derivative);
        }
        return regression;
    }

    private long lastDiskWrites;
    private int lastMemoryFlushes;
    private int lastLogFlushes;

    private long lastSavedQueryDiskReads;
    private long lastSavedMergeDiskReads;
    private long lastMergeDiskReads;
    private long lastMergeReads;

    private long lastWrites;

    private double computeDiskWriteDerivative() {
        double delta = 64 * 1024; // 64MB

        double diskWrites = simulator.cache.getDiskWrites() - lastDiskWrites;

        double numMemoryFlushes = simulator.stats.numMemoryFlushes - lastMemoryFlushes;
        double numLogFlushes = simulator.stats.numLogFlushes - lastLogFlushes;

        double scaleFactor = numMemoryFlushes / (numMemoryFlushes + numLogFlushes);

        double newDiskWrites = diskWrites * Math.log(memoryComponentSize) / Math.log(memoryComponentSize + delta);

        double derivative = scaleFactor * (newDiskWrites - diskWrites) / delta;
        if (derivative > 0) {
            System.out.println("WARNING: wrong write benefit " + derivative);
        }

        return derivative;
    }

    private double computeDiskReadDerivative() {
        double savedQueryDiskReads = simulator.cache.getSavedQueryDiskReads() - lastSavedQueryDiskReads;
        double savedMergeDiskReads = simulator.cache.getSavedMergeDiskReads() - lastSavedMergeDiskReads;
        double mergeDiskReads = simulator.cache.getMergeDiskReads() - lastMergeDiskReads;
        double mergeReads = simulator.cache.getMergeReads() - lastMergeReads;
        double mergeCacheRatio = (mergeDiskReads + savedMergeDiskReads) / mergeReads;

        double mergeWrites = simulator.cache.getDiskWrites() - lastDiskWrites;

        double queryDerivative =
                (savedQueryDiskReads + savedMergeDiskReads) / simulator.config.tuningConfig.simulateSize;
        double mergeDerivative = computeDiskWriteDerivative() / mergeWrites * mergeReads * mergeCacheRatio;

        return queryDerivative + mergeDerivative;
    }

    private void resetStats() {
        lastDiskWrites = simulator.cache.getDiskWrites();
        lastMemoryFlushes = simulator.stats.numMemoryFlushes;
        lastLogFlushes = simulator.stats.numLogFlushes;

        lastSavedQueryDiskReads = simulator.cache.getSavedQueryDiskReads();
        lastSavedMergeDiskReads = simulator.cache.getSavedMergeDiskReads();
        lastMergeDiskReads = simulator.cache.getMergeDiskReads();
        lastMergeReads = simulator.cache.getMergeReads();

        lastWrites = simulator.writes;
    }

}