package edu.uci.asterixdb.storage.sim.lsm;

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
    public static final double MIN_PERCENT = 0.005;
    public static final double INITIAL_PERCENT = 0.05;
    public static final double MAX_PERCENT = 0.1;
    public static final double MIN_COST_CHANGE = 0.001;
    public static final double P = 1.0;
    public static final int HISTORY_SIZE = 3;
    private final Simulator simulator;
    private final int totalMem;

    private ArrayDeque<Point> memoryComponentHistory = new ArrayDeque<>(HISTORY_SIZE);

    public MemoryTuner(Simulator simulator) {
        this.simulator = simulator;
        this.totalMem = simulator.config.tuningConfig.initWriteMemSize + simulator.config.tuningConfig.initCacheSize;

    }

    // we only need to decide the memory allocated to the memory components
    public void tune() {
        double writeDerivative = simulator.config.tuningConfig.writeWeight * computeDiskWriteDerivative();
        double readDerivative = simulator.config.tuningConfig.readWeight * computeDiskReadDerivative(writeDerivative);
        double totalDerivative = writeDerivative + readDerivative;

        double writeCost = normalize(simulator.config.tuningConfig.writeWeight
                * (simulator.cache.getDiskWrites() - lastFlushDiskWrites - lastMergeDiskWrites));
        double readCost = normalize(simulator.config.tuningConfig.readWeight
                * (simulator.cache.getDiskReads() - lastMergeDiskReads - lastQueryDiskReads));
        double totalCost = writeCost + readCost;

        Point p = new Point(simulator.writeMemSize, totalDerivative);
        if (memoryComponentHistory.remove(p)) {
            memoryComponentHistory.add(p);
        } else {
            if (memoryComponentHistory.size() == HISTORY_SIZE) {
                memoryComponentHistory.removeFirst();
            }
            memoryComponentHistory.addLast(p);
        }
        double delta = 0;
        if (memoryComponentHistory.size() > 1) {
            // ideally, totalDerivative should be 0
            SimpleRegression regression = buildRegressionModel();
            delta = Math.abs(P * totalDerivative / regression.getSlope());
        } else {
            delta = Math.abs(INITIAL_PERCENT * totalMem);
        }
        delta = Math.min(delta, totalMem * MAX_PERCENT);

        long oldMemoryComponentSize = simulator.writeMemSize;
        long oldBufferCacheSize = simulator.cacheSize;

        double costChange = Math.abs(delta * totalDerivative / 1024);
        if (simulator.config.tuningConfig.enabled && delta > totalMem * MIN_PERCENT
                && costChange / totalCost > MIN_COST_CHANGE) {
            if (totalDerivative < 0) {
                delta = Math.min(delta, simulator.cacheSize - simulator.config.tuningConfig.minMemorySize);
                // we can make more saving by allocating more memory
                simulator.cacheSize -= delta;
                simulator.updateBufferCacheSize(simulator.cacheSize);
                simulator.writeMemSize += delta;
                simulator.updateMemoryComponentSize(simulator.writeMemSize);
            } else {
                delta = Math.min(delta, simulator.writeMemSize - simulator.config.tuningConfig.minMemorySize);
                simulator.writeMemSize -= delta;
                simulator.updateMemoryComponentSize(simulator.writeMemSize);
                simulator.cacheSize += delta;
                simulator.updateBufferCacheSize(simulator.cacheSize);
            }
        }

        System.out.println(String.format("write derivative %f", writeDerivative));
        System.out.println(String.format("read derivative %f", readDerivative));
        System.out.println(String.format("total derivative %f", totalDerivative));

        System.out.println(String.format("disk write amplification %s",
                simulator.printWriteAmplification(simulator.stats.diskMergeKeys)));
        System.out.println(
                String.format("read cost %.3f, write cost %.3f, total cost %.3f", readCost, writeCost, totalCost));
        System.out.println(String.format("write memory %d MB->%d MB, cache memory %d MB ->%d MB, cost change: %f/%f",
                oldMemoryComponentSize / 1024, simulator.writeMemSize / 1024, oldBufferCacheSize / 1024,
                simulator.cacheSize / 1024, costChange, totalCost));
        System.out.println();

        resetStats();

    }

    private SimpleRegression buildRegressionModel() {
        SimpleRegression regression = new SimpleRegression();

        for (Point p : memoryComponentHistory) {
            regression.addData(p.memory, p.derivative);
        }
        return regression;
    }

    private long lastMergeDiskWrites;
    private long lastFlushDiskWrites;
    private int lastMemoryFlushes;
    private int lastLogFlushes;

    private long lastSavedQueryDiskReads;
    private long lastSavedMergeDiskReads;
    private long lastMergeDiskReads;
    private long lastQueryDiskReads;

    private long lastReads;
    private long lastWrites;

    private double computeDiskWriteDerivative() {
        double totalDerivative = 0;

        for (int i = 0; i < simulator.lsmTrees.length; i++) {
            SimulatedLSM lsm = simulator.lsmTrees[i];

            double mergeDiskWrites = normalize(lsm.stats.mergeDiskWrites);
            if (mergeDiskWrites < 0.00001) {
                continue;
            }
            double numMemoryFlushes = lsm.stats.memoryFlushes;
            double numLogFlushes = lsm.stats.logFlushes;

            double D = lsm.diskLevels.get(lsm.diskLevels.size() - 1).getSize() / 1024;

            double scaleFactor = (lsm.stats.memoryFlushes + lsm.stats.logFlushes) == 0 ? 0
                    : numMemoryFlushes / (numMemoryFlushes + numLogFlushes);
            double a = lsm.stats.getAverageRatio();
            double x = simulator.writeMemSize / 1024;

            double derivative = -scaleFactor * mergeDiskWrites / x / Math.log(D / (a * x));

            if (derivative > 0) {
                System.out.println("WARNING: wrong write benefit " + derivative);
            }
            System.out.println(String.format(
                    "write memory: %.3f/%.3f, memory/log flushes: %.1f/%.1f, disk writes(KB)/op: %.3f, derivative: %f",
                    a * x, x, numMemoryFlushes, numLogFlushes, mergeDiskWrites, derivative));
            totalDerivative += derivative;
        }
        return totalDerivative;
    }

    private double normalize(double value) {
        double operations = simulator.writes + simulator.reads - lastWrites - lastReads;
        return value * simulator.config.tuningConfig.pageKB / operations;
    }

    private double computeDiskReadDerivative(double diskWriteDerivative) {
        double savedQueryDiskReads = normalize(simulator.cache.getSavedQueryDiskReads() - lastSavedQueryDiskReads);
        double savedMergeDiskReads = normalize(simulator.cache.getSavedMergeDiskReads() - lastSavedMergeDiskReads);
        double mergeDiskReads = normalize(simulator.cache.getMergeDiskReads() - lastMergeDiskReads);
        double mergeDiskWrites = normalize(simulator.cache.getMergeDiskWrites() - lastMergeDiskWrites);

        // >=0, allocating more write memory will increase the read cost
        double queryDerivative = savedQueryDiskReads / simulator.config.tuningConfig.simulateSize * 1024;

        double mergeCacheMiss = mergeDiskReads;

        double mergeDerivative = savedMergeDiskReads / simulator.config.tuningConfig.simulateSize * 1024
                + diskWriteDerivative / mergeDiskWrites * mergeCacheMiss;

        System.out.println(String.format("saved query disk read/txn: %f", savedQueryDiskReads));
        System.out.println(String.format("saved merge disk read/txn: %f", savedMergeDiskReads));
        System.out.println(String.format("merge disk reads/txn: %f", mergeDiskReads));
        System.out.println(String.format("merge disk writes/txn: %f", mergeDiskWrites));

        // >=0, allocating more write memory will increase the read cost
        System.out.println(String.format("query derivative: %f", queryDerivative));
        System.out.println(String.format("merge derivative: %f", mergeDerivative));

        return queryDerivative + mergeDerivative;
    }

    private void resetStats() {
        lastMergeDiskWrites = simulator.cache.getMergeDiskWrites();
        lastFlushDiskWrites = simulator.cache.getFlushDiskWrites();
        lastMemoryFlushes = simulator.stats.numMemoryFlushes;
        lastLogFlushes = simulator.stats.numLogFlushes;

        lastSavedQueryDiskReads = simulator.cache.getSavedQueryDiskReads();
        lastSavedMergeDiskReads = simulator.cache.getSavedMergeDiskReads();
        lastMergeDiskReads = simulator.cache.getMergeDiskReads();
        lastQueryDiskReads = simulator.cache.getQueryDiskReads();
        lastReads = simulator.reads;
        lastWrites = simulator.writes;

        for (SimulatedLSM lsm : simulator.lsmTrees) {
            lsm.stats.reset();
        }
    }

    public long getLastOperations() {
        return Math.max(1, lastReads + lastWrites);
    }

    public double getLastDiskReads() {
        return lastMergeDiskReads + lastQueryDiskReads;
    }

    public double getLastDiskWrites() {
        return lastFlushDiskWrites + lastMergeDiskWrites;
    }

}