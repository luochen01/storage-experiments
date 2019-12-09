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
        this.totalMem = simulator.config.tuningConfig.writeMemSize + simulator.config.tuningConfig.cacheSize;

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

        Point p = new Point(simulator.config.tuningConfig.writeMemSize, totalDerivative);
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

        long oldMemoryComponentSize = simulator.config.tuningConfig.writeMemSize;
        long oldBufferCacheSize = simulator.config.tuningConfig.cacheSize;

        double costChange = Math.abs(delta * totalDerivative);
        if (delta > totalMem * MIN_PERCENT && costChange / totalCost > MIN_COST_CHANGE) {
            if (totalDerivative < 0) {
                delta = Math.min(delta,
                        simulator.config.tuningConfig.cacheSize - simulator.config.tuningConfig.minMemorySize);
                // we can make more saving by allocating more memory
                simulator.config.tuningConfig.cacheSize -= delta;
                simulator.updateBufferCacheSize(simulator.config.tuningConfig.cacheSize);
                simulator.config.tuningConfig.writeMemSize += delta;
                simulator.updateMemoryComponentSize(simulator.config.tuningConfig.writeMemSize);
            } else {
                delta = Math.min(delta,
                        simulator.config.tuningConfig.writeMemSize - simulator.config.tuningConfig.minMemorySize);
                simulator.config.tuningConfig.writeMemSize -= delta;
                simulator.updateMemoryComponentSize(simulator.config.tuningConfig.writeMemSize);
                simulator.config.tuningConfig.cacheSize += delta;
                simulator.updateBufferCacheSize(simulator.config.tuningConfig.cacheSize);
            }
        }

        System.out.println(String.format(
                "writes: %d, min lsn: %d, log flushes: %d, memory flushes: %d, Dwrite: %.2f, Dread: %.2f, Dtotal: %.2f, delta: %.2f, cost/total cost: %.2f/%.2f, memory component size: %d->%d, buffer cache size: %d->%d",
                simulator.writes, simulator.minSeq, (simulator.stats.numLogFlushes - lastLogFlushes),
                (simulator.stats.numMemoryFlushes - lastMemoryFlushes), writeDerivative, readDerivative,
                totalDerivative, delta, costChange, totalCost, oldMemoryComponentSize,
                simulator.config.tuningConfig.writeMemSize, oldBufferCacheSize,
                simulator.config.tuningConfig.cacheSize));
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
        double delta = simulator.config.tuningConfig.simulateSize;

        double mergeDiskWrites = normalize(simulator.cache.getMergeDiskWrites() - lastMergeDiskWrites);

        double numMemoryFlushes = simulator.stats.numMemoryFlushes - lastMemoryFlushes;
        double numLogFlushes = simulator.stats.numLogFlushes - lastLogFlushes;

        double totalT = 0;
        double D = 0;
        for (SimulatedLSM lsm : simulator.lsmTrees) {
            double size = lsm.diskLevels.get(lsm.diskLevels.size() - 1).getSize();
            totalT += (size * lsm.config.diskConfig.sizeRatio);
            D += size;
        }

        double T = totalT / D;

        double N = mergeDiskWrites / writeCost(simulator.config.tuningConfig.writeMemSize, D, T);

        double scaleFactor = numMemoryFlushes / (numMemoryFlushes + numLogFlushes);

        double derivative = scaleFactor * N * (writeCost(simulator.config.tuningConfig.writeMemSize + delta, D, T)
                - writeCost(simulator.config.tuningConfig.writeMemSize, D, T)) / delta;

        if (derivative > 0) {
            System.out.println("WARNING: wrong write benefit " + derivative);
        }

        return derivative;
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