package edu.uci.asterixdb.storage.experiments.flowcontrol;

import java.util.ArrayList;
import java.util.List;

public class LevelMergeScheduler implements IOperationScheduler {

    private int numRunningOperations = 0;

    private ILSMSimulator simulator;

    private FlushOperation flushOperation;

    private final List<MergeOperation> mergeOperations = new ArrayList<>();

    private final int toleratedComponentsPerLevel;

    public LevelMergeScheduler() {
        this(1);
    }

    public LevelMergeScheduler(int toleratedComponentsPerLevel) {
        this.toleratedComponentsPerLevel = toleratedComponentsPerLevel;
    }

    @Override
    public void initialize(ILSMSimulator simulator) {
        this.simulator = simulator;
    }

    @Override
    public int getNumRunningOperations() {
        return numRunningOperations;
    }

    @Override
    public int getMaxNumComponents() {
        return 2 + toleratedComponentsPerLevel;
    }

    @Override
    public boolean isFlushable(FlushUnit flushUnit) {
        return !flushUnit.blocked && flushUnit.operation == null && flushUnit.capacity >= flushUnit.flushCapacity;
    }

    @Override
    public FlushOperation scheduleFlushOperation(FlushUnit flushUnit) {
        assert !flushUnit.blocked;
        assert flushUnit.operation == null;
        FlushOperation flushOp = getFlushOperation(flushUnit.flushCapacity);
        flushUnit.operation = flushOp;
        numRunningOperations++;
        return flushOp;
    }

    @Override
    public void completeFlushOperation(FlushUnit flushUnit) {
        FlushOperation flushOp = flushUnit.getOperation();
        flushUnit.capacity -= flushOp.totalCapacity;
        assert flushUnit.capacity >= 0;
        flushUnit.operation = null;
        numRunningOperations--;
        addToMergeUnit(simulator.getMergeUnit(0), flushOp.totalCapacity, flushUnit);

        if (isFlushable(flushUnit)) {
            scheduleFlushOperation(flushUnit);
        }
    }

    protected void addToMergeUnit(MergeUnit mergeUnit, double capacity, Unit sourceUnit) {
        mergeUnit.addComponent(capacity);
        if (mergeUnit.isFull()) {
            mergeUnit.setBlockedUnit(sourceUnit);
            //System.out.println("blocked at level " + mergeUnit.level);
        }
        if (isMergeable(mergeUnit)) {
            scheduleMergeOperation(mergeUnit);
        }
    }

    @Override
    public boolean isMergeable(MergeUnit mergeUnit) {
        return !mergeUnit.blocked && mergeUnit.numComponents >= 2 && mergeUnit.operation == null;
    }

    @Override
    public MergeOperation scheduleMergeOperation(MergeUnit mergeUnit) {
        assert !mergeUnit.blocked;

        MergeOperation mergeOp = getMergeOperation(mergeUnit.level);
        for (int i = 0; i < 2; i++) {
            mergeOp.components[i] = mergeUnit.components[i];
            mergeOp.totalCapacity += mergeOp.components[i];
        }
        mergeUnit.operation = mergeOp;
        numRunningOperations++;
        return mergeOp;
    }

    @Override
    public void completeMergeOperation(MergeUnit mergeUnit, MergeUnit nextUnit) {
        MergeOperation op = mergeUnit.getOperation();
        assert op != null;

        double resultCapacity = op.totalCapacity;
        if (resultCapacity < mergeUnit.maxCapacity) {
            mergeUnit.components[0] = resultCapacity;
            for (int i = 0; i < toleratedComponentsPerLevel; i++) {
                mergeUnit.components[i + 1] = mergeUnit.components[i + 2];
                mergeUnit.components[i + 2] = 0;
            }
            mergeUnit.numComponents -= 1;
            assert mergeUnit.numComponents >= 0;
        } else {
            for (int i = 0; i < toleratedComponentsPerLevel; i++) {
                mergeUnit.components[i] = mergeUnit.components[i + 2];
                mergeUnit.components[i + 2] = 0;
            }
            mergeUnit.numComponents -= 2;
            addToMergeUnit(nextUnit, resultCapacity, mergeUnit);
        }

        if (mergeUnit.blockedUnit != null) {
            mergeUnit.unsetBlockedUnit();
        }
        mergeUnit.operation = null;
        numRunningOperations--;
        if (isMergeable(mergeUnit)) {
            scheduleMergeOperation(mergeUnit);
        }
    }

    @Override
    public void reset() {
        numRunningOperations = 0;
    }

    private MergeOperation getMergeOperation(int level) {
        if (mergeOperations.size() < level + 1) {
            mergeOperations.add(new MergeOperation(new double[2]));
        }
        MergeOperation op = mergeOperations.get(level);
        op.reset();
        return op;
    }

    private FlushOperation getFlushOperation(double totalCapacity) {
        if (flushOperation == null) {
            flushOperation = new FlushOperation(totalCapacity);
        }
        flushOperation.reset(totalCapacity);
        return flushOperation;
    }

}
