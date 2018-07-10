package edu.uci.asterixdb.storage.experiments.flowcontrol;

import java.util.List;

public class NoLimitAllocFlowControlSimulator extends FlowControlSimulator {

    private double flushSpeed;
    private final double[] mergeSpeeds;

    private static int MAX_INGEST_SPEED_FIXED = 15000;

    public NoLimitAllocFlowControlSimulator(int totalCycles) {
        super(totalCycles, new LevelMergeScheduler());
        flushSpeed = Double.MAX_VALUE;
        mergeSpeeds = new double[TOTAL_LEVELS];
        for (int i = 0; i < TOTAL_LEVELS; i++) {
            mergeSpeeds[i] = Double.MAX_VALUE;
        }
    }

    public static double ScaleFactor = 1;

    @Override
    protected double cycle() {
        FlushOperation flushOp = flushUnit.getOperation();
        // process flush op
        double remainingCapacity = MAX_IO_SPEED;
        if (flushOp != null) {
            double processed = MAX_IO_SPEED / scheduler.getNumRunningOperations();
            flushOp.progress(processed);
            remainingCapacity -= processed;
        }

        int maxMergeLevel = 0;
        // process merge
        for (int i = 0; i < TOTAL_LEVELS; i++) {
            MergeUnit unit = mergeUnits.get(i);
            if (unit.numComponents > 0) {
                maxMergeLevel = i;
            }
        }

        double totalRatio = 0;
        // process merge
        for (int i = 0; i <= maxMergeLevel; i++) {
            MergeUnit unit = mergeUnits.get(i);
            MergeOperation op = unit.getOperation();
            if (op != null) {
                double ratio = 1 + op.components[0] / op.components[1];
                totalRatio += ratio;
            }
        }

        // process merge
        for (int i = 0; i < TOTAL_LEVELS; i++) {
            MergeUnit unit = mergeUnits.get(i);
            MergeOperation op = unit.getOperation();
            if (op != null) {
                double ratio = 1 + op.components[0] / op.components[1];
                double processed = remainingCapacity / totalRatio * ratio;
                op.progress(processed);
            }
        }

        // ingest to memory component
        double ingestedData = Double.min(flushUnit.remainingCapacity(), MAX_INGEST_SPEED_FIXED);
        ingestedData = Double.min(ingestedData, getMinIoSpeed());
        flushUnit.capacity += ingestedData;

        // complete ongoing operations and schedule for next cycle
        if (flushOp != null && flushOp.isCompleted()) {
            scheduler.completeFlushOperation(flushUnit);
        }

        // complete ongoing merge operations
        for (int i = TOTAL_LEVELS - 1; i >= 0; i--) {
            MergeUnit unit = mergeUnits.get(i);
            MergeOperation op = unit.getOperation();
            if (op != null && op.isCompleted()) {
                scheduler.completeMergeOperation(unit, mergeUnits.get(i + 1));
            } else if (scheduler.isMergeable(unit)) {
                scheduler.scheduleMergeOperation(unit);
            }
        }
        return ingestedData;
    }

    @Override
    protected void print(int cycle, double data) {
        System.out.println(String.format("%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f", cycle, data, totalIngestedData,
                getFormatMergeSpeed(0), getFormatMergeSpeed(1), getFormatMergeSpeed(2)));
    }

    private double getFormatMergeSpeed(int level) {
        return mergeSpeeds[level] == Double.MAX_VALUE ? 0 : mergeSpeeds[level];
    }

    private void printComponents(StringBuilder sb, List<Double> components) {
        for (int i = 0; i < components.size(); i++) {
            sb.append(components.get(i));
            if (i < components.size() - 1) {
                sb.append('+');
            }
        }
    }

    private double getMinIoSpeed() {
        double min = getFlushIoSpeed(flushUnit);
        for (int i = 0; i < TOTAL_LEVELS; i++) {
            min = Math.min(min, getMergeIoSpeed(mergeUnits.get(i)));
        }
        return min;
    }

    private double getFlushIoSpeed(Unit unit) {
        if (unit.operation != null && unit.operation.cycles > 0) {
            flushSpeed = unit.operation.currentCapacity / unit.operation.cycles;
            return flushSpeed;
        } else {
            return flushSpeed;
        }
    }

    private double getMergeIoSpeed(MergeUnit unit) {
        MergeOperation op = unit.getOperation();
        if (op == null || op.cycles == 0) {
            return mergeSpeeds[unit.level];
        }
        assert op.components[0] >= op.components[1];
        double progress = Math.min(1, op.currentCapacity / op.totalCapacity);
        mergeSpeeds[unit.level] = (progress / op.cycles) * op.components[1];
        return mergeSpeeds[unit.level];
    }

    public static void main(String[] args) {
        for (int speed = 1000; speed <= 11000; speed += 100) {
            MAX_INGEST_SPEED_FIXED = speed;
            double[] values = new NoLimitAllocFlowControlSimulator(360000).execute(false);
            System.out.println(String.format("%d\t%.3f\t%.3f", speed, mean(values), variance(values)));
        }
    }

}
