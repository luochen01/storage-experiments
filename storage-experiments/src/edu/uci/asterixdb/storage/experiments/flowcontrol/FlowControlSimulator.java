package edu.uci.asterixdb.storage.experiments.flowcontrol;

import java.util.ArrayList;
import java.util.List;

public abstract class FlowControlSimulator implements ILSMSimulator {

    protected static final int TOTAL_LEVELS = 10;

    protected static final double FLUSH_CAPACITY = 50000;

    protected static final int NUM_MEMORY_COMPONENTS = 2;

    protected static final int SIZE_RATIO = 10;

    protected static final double MAX_INGEST_SPEED = 50000;

    protected static final double MAX_IO_SPEED = 200000;

    protected final FlushUnit flushUnit = new FlushUnit(FLUSH_CAPACITY * NUM_MEMORY_COMPONENTS, FLUSH_CAPACITY);

    protected final List<MergeUnit> mergeUnits = new ArrayList<>();

    private final int totalCycles;

    protected double totalIngestedData;

    protected final IOperationScheduler scheduler;

    public FlowControlSimulator(int totalCycles, IOperationScheduler scheduler) {
        double currentCapacity = FLUSH_CAPACITY * SIZE_RATIO;
        for (int i = 0; i < TOTAL_LEVELS; i++) {
            MergeUnit mergeUnit = new MergeUnit(i, scheduler.getMaxNumComponents(), currentCapacity);
            currentCapacity = currentCapacity * SIZE_RATIO;
            mergeUnits.add(mergeUnit);
        }
        mergeUnits.add(null);
        this.totalCycles = totalCycles;
        this.totalIngestedData = 0;
        this.scheduler = scheduler;
        this.scheduler.initialize(this);
    }

    public double[] execute(boolean print) {
        double[] results = new double[totalCycles];
        for (int i = 0; i < totalCycles; i++) {
            double data = cycle();
            totalIngestedData += data;
            if (print) {
                print(i, data);
            }
            results[i] = data;
        }
        return results;
    }

    protected abstract double cycle();

    protected void print(int cycle, double data) {
        System.out.println(String.format("%d\t%.3f\t%.3f", cycle, data, totalIngestedData));
    }

    @Override
    public FlushUnit getFlushUnit() {
        return flushUnit;
    }

    @Override
    public MergeUnit getMergeUnit(int level) {
        return mergeUnits.get(level);
    }

    public static double variance(double[] values) {
        double variance = 0;
        double mean = mean(values);
        for (double d : values) {
            variance = variance + (mean - d) * (mean - d);
        }
        return Math.sqrt(variance / values.length);

    }

    public static double mean(double[] values) {
        double total = 0;
        for (double d : values) {
            total += d;
        }
        return total / values.length;
    }

}
