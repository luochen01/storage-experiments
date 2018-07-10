package edu.uci.asterixdb.storage.experiments.flowcontrol;

public class SyncLimitFlowControlSimulator extends FlowControlSimulator {

    public SyncLimitFlowControlSimulator(int totalCycles) {
        super(totalCycles, new LevelMergeScheduler());

    }

    private static final double delta = 100;

    private double lastIngestedData = MAX_INGEST_SPEED;

    @Override
    protected double cycle() {
        FlushOperation flushOp = flushUnit.getOperation();
        double oldFlushCapacity = flushOp != null ? flushOp.currentCapacity : 0;

        double remainingBudget = MAX_IO_SPEED;

        boolean changed = true;
        while ((remainingBudget >= 0) && changed) {
            changed = false;
            // process flush op
            if (flushOp != null && !flushOp.isCompleted()
                    && flushOp.currentCapacity / flushOp.totalCapacity < getInProgress(0)) {
                flushOp.currentCapacity += delta;
                remainingBudget -= delta;
                changed = true;
            }

            // process merge
            for (int i = 0; i < TOTAL_LEVELS - 1; i++) {
                MergeUnit unit = mergeUnits.get(i);
                MergeOperation op = unit.getOperation();
                if (op != null && !op.isCompleted() && getOutProgress(i) < getInProgress(i + 1)) {
                    op.currentCapacity += delta;
                    remainingBudget -= delta;
                    changed = true;
                }
            }
        }

        double newFlushCapacity = flushOp != null ? flushOp.currentCapacity : Double.MAX_VALUE;

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

        // complete ongoing operations and schedule for next cycle
        if (flushOp != null && flushOp.isCompleted()) {
            scheduler.completeFlushOperation(flushUnit);
        }

        // ingest to memory component
        double flushedCapacity = 0;
        if (flushOp == null) {
            flushedCapacity = lastIngestedData;
            lastIngestedData *= 1.01;
        } else {
            flushedCapacity = newFlushCapacity - oldFlushCapacity;
            lastIngestedData = flushedCapacity;
        }
        double ingestedData = Double.min(Double.max(100, flushedCapacity), MAX_INGEST_SPEED);
        ingestedData = Double.min(ingestedData, flushUnit.remainingCapacity());
        if (flushOp == null) {
            lastIngestedData = ingestedData;
        } else {
            ingestedData = lastIngestedData;
            lastIngestedData *= 1.05;
        }
        flushUnit.capacity += ingestedData;

        if (scheduler.isFlushable(flushUnit)) {
            scheduler.scheduleFlushOperation(flushUnit);
        }

        return ingestedData;
    }

    /**
     * Invariant: for each level L, its out progress should be <= the in progress of level L+1
     *
     * @param level
     * @return
     */
    private double getInProgress(int level) {
        MergeUnit unit = mergeUnits.get(level);
        MergeOperation op = unit.getOperation();
        if (op == null) {
            return Double.MAX_VALUE;
        } else {
            return op.currentCapacity / op.totalCapacity;
        }
    }

    private double getOutProgress(int level) {
        MergeUnit unit = mergeUnits.get(level);
        if (unit.numComponents == 0) {
            return 0;
        } else {
            double inProgress = unit.getOperation() != null
                    ? unit.getOperation().currentCapacity / unit.getOperation().totalCapacity
                    : 0;
            return unit.components[0] / unit.maxCapacity + inProgress;
        }
    }

    public static void main(String[] args) {
        new SyncLimitFlowControlSimulator(3600).execute(true);
    }

}
