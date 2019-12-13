package edu.uci.asterixdb.storage.sim;

import java.io.IOException;
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
    public static boolean OPT_MULTI_LSM = true;

    public static final double MIN_PERCENT = 0.005;
    public static final double INITIAL_PERCENT = 0.05;
    public static final double MAX_PERCENT = 0.1;
    public static final double MIN_COST_CHANGE = 0.005;
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
        double readDerivative = simulator.config.tuningConfig.readWeight * computeDiskReadDerivative();
        double totalDerivative = writeDerivative + readDerivative;

        double totalCost = normalize(simulator.config.tuningConfig.writeWeight
                * (simulator.cache.getDiskWrites() - lastFlushDiskWrites - lastMergeDiskWrites)
                + simulator.config.tuningConfig.readWeight
                        * (simulator.cache.getDiskReads() - lastMergeDiskReads - lastQueryDiskReads));

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

        double costChange = Math.abs(delta * totalDerivative);
        if (delta > totalMem * MIN_PERCENT && costChange / totalCost > MIN_COST_CHANGE) {
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

        System.out.println(String.format(
                "writes: %d, min lsn: %d, log flushes: %d, memory flushes: %d, Dwrite: %.2f, Dread: %.2f, Dtotal: %.2f, delta: %.2f, cost/total cost: %.2f/%.2f, memory component size: %d->%d, buffer cache size: %d->%d",
                simulator.writes, simulator.minSeq, (simulator.stats.numLogFlushes - lastLogFlushes),
                (simulator.stats.numMemoryFlushes - lastMemoryFlushes), writeDerivative, readDerivative,
                totalDerivative, delta, costChange, totalCost, oldMemoryComponentSize, simulator.writeMemSize,
                oldBufferCacheSize, simulator.cacheSize));
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

    private long lastWrites;

    private double computeDiskWriteDerivative() {
        if (OPT_MULTI_LSM) {
            return optDiskWriteDerivative();
        } else {
            return simpleDiskWriteDerivative();
        }
    }

    private double simpleDiskWriteDerivative() {
        double mergeDiskWrites = normalize(simulator.cache.getMergeDiskWrites() - lastMergeDiskWrites);

        double numMemoryFlushes = simulator.stats.numMemoryFlushes - lastMemoryFlushes;
        double numLogFlushes = simulator.stats.numLogFlushes - lastLogFlushes;

        double D = 0;
        for (SimulatedLSM lsm : simulator.lsmTrees) {
            double size = lsm.diskLevels.get(lsm.diskLevels.size() - 1).getSize();
            D += size;
        }

        double scaleFactor = numMemoryFlushes / (numMemoryFlushes + numLogFlushes);
        double x = simulator.writeMemSize;

        double derivative = -scaleFactor * mergeDiskWrites / x / Math.log(D / x);

        if (derivative > 0) {
            System.out.println("WARNING: wrong write benefit " + derivative);
        }
        return derivative;
    }

    private double optDiskWriteDerivative() {
        double totalDerivative = 0;

        for (int i = 0; i < simulator.lsmTrees.length; i++) {
            SimulatedLSM lsm = simulator.lsmTrees[i];

            double mergeDiskWrites = normalize(lsm.stats.mergeDiskWrites);
            if (mergeDiskWrites < 0.00001) {
                continue;
            }
            double numMemoryFlushes = lsm.stats.memoryFlushes;
            double numLogFlushes = lsm.stats.logFlushes;

            double D = lsm.diskLevels.get(lsm.diskLevels.size() - 1).getSize();

            double scaleFactor = (lsm.stats.memoryFlushes + lsm.stats.logFlushes) == 0 ? 0
                    : numMemoryFlushes / (numMemoryFlushes + numLogFlushes);
            double a = lsm.stats.getAverageRatio();
            double x = simulator.writeMemSize;

            double derivative = -scaleFactor * mergeDiskWrites / x / Math.log(D / (a * x));

            if (derivative > 0) {
                System.out.println("WARNING: wrong write benefit " + derivative);
            }
            totalDerivative += derivative;
        }
        return totalDerivative;
    }

    private double normalize(double value) {
        double writes = simulator.writes - lastWrites;
        return value / writes * simulator.config.tuningConfig.tuningCycle;
    }

    private double computeDiskReadDerivative() {
        double savedQueryDiskReads = normalize(simulator.cache.getSavedQueryDiskReads() - lastSavedQueryDiskReads);
        double savedMergeDiskReads = normalize(simulator.cache.getSavedMergeDiskReads() - lastSavedMergeDiskReads);
        double mergeDiskReads = normalize(simulator.cache.getMergeDiskReads() - lastMergeDiskReads);
        double mergeDiskWrites = normalize(simulator.cache.getMergeDiskWrites() - lastMergeDiskWrites);

        // >=0, allocating more write memory will increase the read cost
        double queryDerivative = savedQueryDiskReads / simulator.config.tuningConfig.simulateSize;

        double mergeCacheMiss = mergeDiskReads;

        double mergeDerivative = savedMergeDiskReads / (simulator.config.tuningConfig.simulateSize)
                + computeDiskWriteDerivative() / mergeDiskWrites * mergeCacheMiss;

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
        lastWrites = simulator.writes;

        for (SimulatedLSM lsm : simulator.lsmTrees) {
            lsm.stats.reset();
        }
    }

    public static void main(String[] args) throws IOException {
        double T = 10;
        // 1M keys
        double D = 10 * 1000;

        System.out.println(writeCost(1000, D, T));

        //        double[] xs = new double[10000];
        //        double[] ys = new double[xs.length];
        //        for (int i = 0; i < xs.length; i++) {
        //            xs[i] = i + 1;
        //            ys[i] = writeCost(xs[i], D, T);
        //        }
        //        XYChart chart = QuickChart.getChart("write(x)", "x", "write(x)", "write(x)", xs, ys);
        //
        //        BitmapEncoder.saveBitmapWithDPI(chart, "./write-cost.png", BitmapFormat.PNG, 300);
    }

    private static double log(double base, double value) {
        return Math.log(value) / Math.log(base);
    }

    private static double writeCost(double x, double D, double T) {
        //        double levels = Math.floor(log(T, D / x));
        //        if (levels >= 0) {
        //            return (T + 1) * (levels) + D / (x * Math.pow(T, levels)) + 1;
        //        } else {
        //            return D / x + 1;
        //        }

        double levels = log(T, D / x);
        if (levels >= 0) {
            return (T + 1) * (levels);
        } else {
            return D / x + 1;
        }
    }

}