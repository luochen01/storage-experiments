package edu.uci.asterixdb.storage.experiments.flowcontrol;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LevelFlowControlSpeedSolver implements ILSMSimulator {

    private static final Logger LOGGER = LogManager.getLogger(LevelFlowControlSpeedSolver.class);

    protected final int numLevels;

    protected final double memoryComponentCapacity;

    protected final int numMemoryComponents;

    protected final int sizeRatio;

    protected final double maxIoSpeed;

    protected final FlushUnit flushUnit;

    protected final MergeUnit[] mergeUnits;

    protected final IOperationScheduler scheduler;

    protected static final boolean RUNTIME_STAT_ENABLED = false;

    protected final double initialUsedMemoryCapacity;

    protected final double initialFlushedCapacity;

    protected final double[][] initialUsedComponentsCapacities;

    protected final double[] initialMergedCapacities;

    // one day
    private static long MAX_CYCLES = 30 * 24 * 3600;

    public LevelFlowControlSpeedSolver(int smallComponentsPerMerge, int numLevels, double memoryComponentCapacity,
            int numMemoryComponents, int sizeRatio, double maxIoSpeed, double usedMemoryCapacity,
            double flushedCapacity, double[][] usedComponentsCapacities, double[] mergedCapacities) {
        this.numLevels = numLevels;
        this.mergeUnits = new MergeUnit[numLevels];
        this.memoryComponentCapacity = memoryComponentCapacity;
        this.numMemoryComponents = numMemoryComponents;
        this.sizeRatio = sizeRatio;
        this.maxIoSpeed = maxIoSpeed;
        this.scheduler = new LevelMergeScheduler(smallComponentsPerMerge);
        this.scheduler.initialize(this);

        this.flushUnit = new FlushUnit(memoryComponentCapacity * numMemoryComponents, memoryComponentCapacity);
        if (RUNTIME_STAT_ENABLED) {
            this.flushUnit.capacity = usedMemoryCapacity;
        }
        this.initialUsedMemoryCapacity = usedMemoryCapacity;
        this.initialFlushedCapacity = flushedCapacity;
        this.initialUsedComponentsCapacities = usedComponentsCapacities;
        this.initialMergedCapacities = mergedCapacities;
        assert usedComponentsCapacities.length == numLevels;
        double maxCapacity = memoryComponentCapacity * sizeRatio;
        for (int i = 0; i < numLevels; i++) {
            MergeUnit unit = new MergeUnit(i, scheduler.getMaxNumComponents(), maxCapacity);
            maxCapacity *= sizeRatio;
            mergeUnits[i] = unit;
        }

        reset();
    }

    @Override
    public FlushUnit getFlushUnit() {
        return flushUnit;
    }

    @Override
    public MergeUnit getMergeUnit(int level) {
        return mergeUnits[level];
    }

    public int solveMaxSpeed() {
        long begin = System.nanoTime();

        // perform a binary search between lastSpeed and currentSpeed
        int low = 0;
        int high = (int) maxIoSpeed;
        boolean changed = true;
        while (changed) {
            changed = false;
            int mid = (low + high) >>> 1;
            boolean midVal = simulate(mid);
            if (midVal) {
                int newHigh = mid;
                if (high != newHigh) {
                    changed = true;
                    high = newHigh;
                }
            } else {
                int newLow = mid;
                if (low != newLow) {
                    changed = true;
                    low = newLow;
                }
            }
        }

        //TODO this function may not be monotonic
        assert !simulate(low);

        long end = System.nanoTime();
        LOGGER.error("Find max speed {} takes {} ms", low, TimeUnit.NANOSECONDS.toMillis(end - begin));
        return low;
    }

    public int solveMaxBlockingSpeed() {
        return (int) nonStoppingSimulate(Integer.MAX_VALUE, false);
    }

    /**
     *
     * @param speed
     * @return whether blocking occurs
     */
    protected boolean simulate(int speed) {
        reset();
        try {
            for (int i = 0; i < MAX_CYCLES; i++) {
                double data = cycle(speed);
                if (data < speed) {
                    return true;
                }
            }
        } catch (IndexOutOfBoundsException e) {
        }
        return false;
    }

    protected double nonStoppingSimulate(int speed, boolean print) {
        reset();
        int i = 0;
        double total = 0;
        try {
            for (; i < MAX_CYCLES; i++) {
                double data = cycle(speed);
                total += data;
                if (print) {
                    System.out.println(
                            String.format("%d\t%.3f\t%.3f\t%.3f\t%s\t%s\t%s\t%s", i, data, total, flushUnit.capacity,
                                    getComponents(0), getComponents(1), getComponents(2), getComponents(3)));
                }
            }
        } catch (IndexOutOfBoundsException e) {
        }
        return total / i;
    }

    //    @Override
    //    protected void print(int cycle, double data) {
    //        System.out.println(String.format("%d\t%.3f\t%.3f\t%s\t%s\t%s\t%s", cycle, data, totalIngestedData,
    //                getComponents(0), getComponents(1), getComponents(2), getComponents(3)));
    //    }

    private String getComponents(int level) {
        MergeUnit unit = mergeUnits[level];
        if (unit.numComponents >= 2) {
            double ratio = unit.components[0] / unit.maxCapacity * sizeRatio;
            return String.format("%.2f", 1 / (ratio + 1));
        } else {
            return "-";
        }
    }

    protected double cycle(int speed) {

        FlushOperation flushOp = flushUnit.getOperation();

        double remainingCapacity = maxIoSpeed;
        // process flush op
        if (flushOp != null) {
            double processed = maxIoSpeed / scheduler.getNumRunningOperations();
            flushOp.currentCapacity += processed;
            remainingCapacity -= processed;
        }
        double totalComponents = 0;
        // process merge
        for (int i = 0; i < numLevels; i++) {
            MergeUnit unit = mergeUnits[i];
            MergeOperation op = unit.getOperation();
            if (op != null) {
                totalComponents += (1 - op.currentCapacity / op.totalCapacity + unit.numComponents - 2);
                if (i == 0) {
                    totalComponents += flushUnit.capacity / flushUnit.flushCapacity;
                }
            }
        }
        // process merge
        for (int i = 0; i < numLevels; i++) {
            MergeUnit unit = mergeUnits[i];
            MergeOperation op = unit.getOperation();
            if (op != null) {
                //                double processed = remainingCapacity
                //                        * (1 - op.currentCapacity / op.totalCapacity + unit.numComponents - 2) / totalComponents;
                double processed = maxIoSpeed / scheduler.getNumRunningOperations();
                op.currentCapacity += processed;
            }
        }

        // complete ongoing operations and schedule for next cycle
        if (flushOp != null && flushOp.isCompleted()) {
            scheduler.completeFlushOperation(flushUnit);
        } else if (scheduler.isFlushable(flushUnit)) {
            scheduler.scheduleFlushOperation(flushUnit);
        }

        // complete ongoing merge operations
        for (int i = numLevels - 1; i >= 0; i--) {
            MergeUnit unit = mergeUnits[i];
            MergeOperation op = unit.getOperation();
            if (op != null && op.isCompleted()) {
                scheduler.completeMergeOperation(unit, mergeUnits[i + 1]);
            } else if (scheduler.isMergeable(unit)) {
                scheduler.scheduleMergeOperation(unit);
            }
        }
        // ingest to memory component
        double ingestedData = Double.min(flushUnit.remainingCapacity(), speed);
        flushUnit.capacity += ingestedData;

        return ingestedData;
    }

    protected void reset() {
        scheduler.reset();
        flushUnit.reset(initialUsedMemoryCapacity);
        if (scheduler.isFlushable(flushUnit)) {
            scheduler.scheduleFlushOperation(flushUnit);
        }
        if (initialFlushedCapacity > 0) {
            flushUnit.operation.currentCapacity = initialFlushedCapacity;
        }
        for (int i = 0; i < numLevels; i++) {
            mergeUnits[i].reset(initialUsedComponentsCapacities[i]);
            if (scheduler.isMergeable(mergeUnits[i])) {
                scheduler.scheduleMergeOperation(mergeUnits[i]);
            }
            if (initialMergedCapacities[i] > 0) {
                mergeUnits[i].getOperation().currentCapacity = initialMergedCapacities[i];
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int numMemoryComponents = 4;
        int sizeRatio = 10;
        int levels = 5;
        int smallComponentsPerMerge = 1;
        // vary levels

        PrintStream out = null;
        //
        //        out = new PrintStream(
        //                new File("/Users/luochen/Documents/Research/experiments/results/flowcontrol/simulation/level.csv"));
        //        for (int l = 1; l <= 7; l++) {
        //
        //            LevelFlowControlSpeedSolver solver = new LevelFlowControlSpeedSolver(smallComponentsPerMerge, l,
        //                    FlowControlSimulator.FLUSH_CAPACITY, numMemoryComponents, sizeRatio,
        //                    FlowControlSimulator.MAX_IO_SPEED, 0, 0, new double[levels][0], new double[levels]);
        //            int speed = solver.solveMaxSpeed();
        //            int maxSpeed = solver.solveMaxBlockingSpeed();
        //            out.println(l + "\t" + speed + "\t" + maxSpeed);
        //        }
        //        out.close();

        out = new PrintStream(new File(
                "/Users/luochen/Documents/Research/experiments/results/flowcontrol/simulation/memorycomponents.csv"));
        for (int n = 2; n <= 100000; n *= 2) {

            LevelFlowControlSpeedSolver solver = new LevelFlowControlSpeedSolver(smallComponentsPerMerge, levels,
                    FlowControlSimulator.FLUSH_CAPACITY, n, sizeRatio, FlowControlSimulator.MAX_IO_SPEED, 0, 0,
                    new double[levels][0], new double[levels]);
            int speed = solver.solveMaxSpeed();
            int maxSpeed = solver.solveMaxBlockingSpeed();
            out.println(n + "\t" + speed + "\t" + maxSpeed);
        }
        out.close();

        //        out = new PrintStream(
        //                new File("/Users/luochen/Documents/Research/experiments/results/flowcontrol/simulation/sizeratio.csv"));
        //        for (int t = 2; t <= 10; t++) {
        //            LevelFlowControlSpeedSolver solver = new LevelFlowControlSpeedSolver(smallComponentsPerMerge, levels,
        //                    FlowControlSimulator.FLUSH_CAPACITY, numMemoryComponents, t, FlowControlSimulator.MAX_IO_SPEED, 0,
        //                    0, new double[levels][0], new double[levels]);
        //            int speed = solver.solveMaxSpeed();
        //            int maxSpeed = solver.solveMaxBlockingSpeed();
        //            out.println(t + "\t" + speed + "\t" + maxSpeed);
        //        }
        //        out.close();

        //        out = new PrintStream(new File(
        //                "/Users/luochen/Documents/Research/experiments/results/flowcontrol/simulation/sizeratio-2small-merge.csv"));
        //        for (int t = 2; t <= 10; t++) {
        //            LevelFlowControlSpeedSolver solver =
        //                    new LevelFlowControlSpeedSolver(2, levels, FlowControlSimulator.FLUSH_CAPACITY, 8, t,
        //                            FlowControlSimulator.MAX_IO_SPEED, 0, 0, new double[levels][0], new double[levels]);
        //            int speed = solver.solveMaxSpeed();
        //            int maxSpeed = solver.solveMaxBlockingSpeed();
        //            out.println(t + "\t" + speed + "\t" + maxSpeed);
        //        }
        //        out.close();

        //        out = new PrintStream(new File(
        //                "/Users/luochen/Documents/Research/experiments/results/flowcontrol/simulation/small-components-merge.csv"));
        //        for (int t = 1; t <= 10; t++) {
        //            LevelFlowControlSpeedSolver solver = new LevelFlowControlSpeedSolver(t, levels,
        //                    FlowControlSimulator.FLUSH_CAPACITY, numMemoryComponents, sizeRatio,
        //                    FlowControlSimulator.MAX_IO_SPEED, 0, 0, new double[levels][0], new double[levels]);
        //            int speed = solver.solveMaxSpeed();
        //            int maxSpeed = solver.solveMaxBlockingSpeed();
        //            out.println(t + "\t" + speed + "\t" + maxSpeed);
        //        }
        //        out.close();

    }
}
