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
}

class MemoryTuner {
    public static final double P = 0.5;
    public static final int HISTORY_SIZE = 10;
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
        double writeDerivative = computeDiskWriteDerivative() * simulator.config.tuningConfig.writes;
        double readDerivative = computeDiskReadDerivative() * simulator.config.tuningConfig.reads;

        double totalDerivative = writeDerivative - readDerivative;

        if (memoryComponentHistory.size() == HISTORY_SIZE) {
            memoryComponentHistory.removeFirst();
        }
        memoryComponentHistory.addLast(new Point(memoryComponentSize, totalDerivative));
        double delta = 0;
        if (memoryComponentHistory.size() > 1) {
            // ideally, totalDerivative should be 0
            SimpleRegression regression = buildRegressionModel();
            delta = Math.abs(P / regression.getSlope() * totalDerivative);
        } else {
            delta = Math.abs(P / totalDerivative);
        }

        if (totalDerivative > 0) {
            // we can make more saving more allocating more memory
            bufferCacheSize -= delta;
            simulator.updateBufferCacheSize(bufferCacheSize);
            memoryComponentSize += delta;
            simulator.updateMemoryComponentSize(memoryComponentSize);
        } else {
            memoryComponentSize -= delta;
            simulator.updateMemoryComponentSize(memoryComponentSize);
            bufferCacheSize += delta;
            simulator.updateBufferCacheSize(bufferCacheSize);
        }

        resetStats();
    }

    private SimpleRegression buildRegressionModel() {
        SimpleRegression regression = new SimpleRegression();

        for (Point p : memoryComponentHistory) {
            regression.addData(p.memory, p.derivative);
        }
        return regression;
    }

    private long lastDiskMergedKeys;
    private int lastMemoryFlushes;
    private int lastLogFlushes;

    private long lastSavedQueryDiskReads;
    private long lastSavedMergeDiskReads;
    private long lastMergeDiskReads;
    private long lastMergeReads;

    private double computeDiskWriteDerivative() {
        double delta = 64 * 1024; // 64MB

        double mergeKeys = simulator.stats.diskMergeKeys - lastDiskMergedKeys;

        double numMemoryFlushes = simulator.stats.numMemoryFlushes - lastMemoryFlushes;
        double numLogFlushes = simulator.stats.numLogFlushes - lastLogFlushes;

        double scaleFactor = numMemoryFlushes / (numMemoryFlushes + numLogFlushes);

        double newMergedKeys =
                mergeKeys * Math.log(memoryComponentSize) / Math.log(memoryComponentSize + lastDiskMergedKeys);

        double benefit = scaleFactor * (mergeKeys - newMergedKeys) / delta;

        return benefit;
    }

    private double computeDiskReadDerivative() {
        double savedQueryDiskReads = simulator.cache.getSavedQueryDiskReads() - lastSavedQueryDiskReads;
        double savedMergeDiskReads = simulator.cache.getSavedMergeDiskReads() - lastSavedMergeDiskReads;
        double mergeDiskReads = simulator.cache.getMergeDiskReads() - lastMergeDiskReads;
        double mergeReads = simulator.cache.getMergeReads() - lastMergeReads;
        double mergeCacheRatio = (mergeDiskReads + savedMergeDiskReads) / mergeReads;

        double queryDerivative = savedQueryDiskReads / simulator.cache.getSimulateCacheSize();
        double mergeDerivative = computeDiskWriteDerivative() * mergeCacheRatio;

        return queryDerivative - mergeDerivative;
    }

    private void resetStats() {
        lastDiskMergedKeys = simulator.stats.diskMergeKeys;
        lastMemoryFlushes = simulator.stats.numMemoryFlushes;
        lastLogFlushes = simulator.stats.numLogFlushes;

        lastSavedQueryDiskReads = simulator.cache.getSavedQueryDiskReads();
        lastSavedMergeDiskReads = simulator.cache.getSavedMergeDiskReads();
        lastMergeDiskReads = simulator.cache.getMergeDiskReads();
        lastMergeReads = simulator.cache.getMergeReads();
    }

}