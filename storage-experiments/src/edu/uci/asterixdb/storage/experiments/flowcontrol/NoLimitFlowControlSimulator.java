package edu.uci.asterixdb.storage.experiments.flowcontrol;

public class NoLimitFlowControlSimulator extends FlowControlSimulator {

    public NoLimitFlowControlSimulator(int totalCycles) {
        super(totalCycles, new LevelMergeScheduler());

    }

    private static double MAX_INGEST_SPEED_FIXED = 15000;

    @Override
    protected double cycle() {
        // ingest to memory component
        double ingestedData = Double.min(flushUnit.remainingCapacity(), MAX_INGEST_SPEED_FIXED);
        flushUnit.capacity += ingestedData;

        FlushOperation flushOp = flushUnit.getOperation();

        // process flush op
        if (flushOp != null) {
            double processed = MAX_IO_SPEED / scheduler.getNumRunningOperations();
            flushOp.currentCapacity += processed;
        }

        // process merge
        for (int i = 0; i < TOTAL_LEVELS; i++) {
            MergeUnit unit = mergeUnits.get(i);
            MergeOperation op = unit.getOperation();
            if (op != null) {
                double processed = MAX_IO_SPEED / scheduler.getNumRunningOperations();
                op.currentCapacity += processed;
            }
        }

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

    //    @Override
    //    protected void print(int cycle, double data) {
    //        System.out.println(String.format("%d\t%.3f\t%.3f\t%s\t%s\t%s\t%s", cycle, data, totalIngestedData,
    //                getComponents(0), getComponents(1), getComponents(2), getComponents(3)));
    //    }

    private String getComponents(int level) {
        MergeUnit unit = mergeUnits.get(level);
        if (unit.numComponents >= 2) {
            double ratio = unit.components[0] / unit.maxCapacity * SIZE_RATIO;
            return String.format("%.2f", 1 / (ratio + 1));
        } else {
            return "-";
        }
    }

    public static void main(String[] args) {
        MAX_INGEST_SPEED_FIXED = 11000;
        double[] values = new NoLimitFlowControlSimulator(3600 * 3).execute(true);
    }

    private static String print(double[] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("average = ");
        sb.append(mean(values));
        sb.append(" variance = ");
        sb.append(variance(values));
        return sb.toString();
    }

}
