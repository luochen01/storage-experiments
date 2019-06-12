/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.uci.asterixdb.storage.experiments.lsm.simulator;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.SimulateTerminateException;

public abstract class AbstractOperationScheduler implements IOperationScheduler {

    protected static final Logger LOGGER = LogManager.getLogger(AbstractOperationScheduler.class);

    protected int numRunningOperations = 0;

    protected final int toleratedComponentsPerLevel;
    protected final int numLevels;

    protected final RandomVariable memoryComponentCapacity;
    protected RandomVariable memoryComponentFillUpTime;
    protected final int totalMemoryComponents;
    protected final int sizeRatio;
    protected final ISpeedProvider flushSpeed;
    protected final ISpeedProvider[] mergeSpeeds;
    protected final ILSMFinalizingPagesEstimator pageEstimator;

    protected final double subOperationProcessingRecords;
    protected final double subOperationPages;
    protected final double baseLevelCapacity;

    protected final FlushUnit flushUnit;
    protected final MergeUnit[] mergeUnits;
    protected FlushOperation flushOperation;
    protected final MergeOperation[] mergeOperations;

    protected final OperationQueue opQueue = new OperationQueue();

    protected final SubOperation[] newSubOperations;
    protected int numNewSubOperations = 0;

    protected final IoOperation[] newIoOperations;
    protected int numNewIoOperations = 0;
    protected int runningMerges = 0;

    protected static final int INGEST_SUB_OP = 0;
    protected static final int FLUSH_SUB_OP = 1;
    protected static final int MERGE_SUB_OP_START = 2;

    protected double time;
    protected double data;

    public static final int[] TOTAL_MERGES = new int[10];
    public static final double[] TOTAL_MERGE_CURRENT_LEVEL_COSTS = new double[10];
    public static final double[] TOTAL_MERGE_NEXT_LEVEL_COSTS = new double[10];

    public AbstractOperationScheduler(int toleratedComponentsPerLevel, int numLevels,
            RandomVariable memoryComponentCapacity, int totalMemoryComponents, int sizeRatio, ISpeedProvider flushSpeed,
            ISpeedProvider[] mergeSpeeds, ILSMFinalizingPagesEstimator pageEstimator,
            double subOperationProcessingRecords, double subOperationPages, double baseLevelCapacity) {
        this.toleratedComponentsPerLevel = toleratedComponentsPerLevel;
        this.numLevels = numLevels;
        this.memoryComponentCapacity = memoryComponentCapacity;
        this.totalMemoryComponents = totalMemoryComponents;
        this.sizeRatio = sizeRatio;
        this.flushSpeed = flushSpeed;
        this.mergeSpeeds = mergeSpeeds;
        this.pageEstimator = pageEstimator;
        this.subOperationProcessingRecords = subOperationProcessingRecords;
        this.subOperationPages = subOperationPages;
        this.baseLevelCapacity = baseLevelCapacity;

        this.flushUnit =
                new FlushUnit(totalMemoryComponents, memoryComponentCapacity, new SubOperation(INGEST_SUB_OP, this));
        this.flushOperation = new FlushOperation(0, new SubOperation(FLUSH_SUB_OP, this), this, flushSpeed);
        this.mergeUnits = new MergeUnit[numLevels];
        this.mergeOperations = new MergeOperation[numLevels];

        this.newSubOperations = new SubOperation[numLevels + 2];
        this.newIoOperations = new IoOperation[numLevels + 2];
    }

    public void initialize() {
        // initialize merge units and operations
        for (int i = 0; i < mergeUnits.length; i++) {
            mergeUnits[i] = createMergeUnit(i);
        }

        for (int i = 0; i < mergeUnits.length; i++) {
            mergeOperations[i] =
                    createMergeOperaiton(i, mergeSpeeds[i], new SubOperation(i + MERGE_SUB_OP_START, this));
        }
    }

    /**
     * @param speed
     * @return whether blocking occurs
     */
    public boolean simulate(double speed, double maxTime, MutableDouble totalData, boolean stopOnStalls) {
        try {
            if (numLevels == 0) {
                return false;
            }
            if (flushUnit.isFull()) {
                // already stalled
                LOGGER.error("speed {} stops after time {}, stalled: {}", speed, time, true);
                return true;
            }

            // continuous time in seconds
            time = 0;
            data = 0;
            boolean stalled = false;
            SimulateTerminateException exception = null;

            try {
                while (true) {
                    SubOperation top = opQueue.peek();
                    assert top != null;
                    double duration = top.remainingTime;
                    assert duration >= 0 && !Double.isNaN(duration);

                    int size = opQueue.size();
                    top.remainingTime = 0;
                    assert top.isCompleted();
                    for (int i = 1; i < size; i++) {
                        SubOperation op = opQueue.get(i);
                        op.remainingTime -= duration;
                        assert op.remainingTime >= 0;
                    }
                    completeSubOperation(top);
                    if (flushUnit.isFull() && stopOnStalls) {
                        stalled = true;
                        break;
                    }
                    for (int i = 0; i < numNewIoOperations; i++) {
                        // start progress new io operations
                        newIoOperations[i].initializeNewSubOperation();
                    }
                    numNewIoOperations = 0;
                    if (numNewSubOperations > 0) {
                        opQueue.replaceTop(newSubOperations[0]);
                        for (int i = 1; i < numNewSubOperations; i++) {
                            opQueue.add(newSubOperations[i]);
                        }
                        numNewSubOperations = 0;
                    } else {
                        opQueue.removeTop();
                    }

                    time += duration;
                    if (time > maxTime) {
                        break;
                    }

                }
            } catch (SimulateTerminateException e) {
                exception = e;
            }
            LOGGER.error("speed {} stops after time {}, stalled: {}, level: {}, numEffectiveLevels: {}", speed, time,
                    stalled, exception != null ? exception.level : "?", numLevels);
            return stalled;
        } finally {
            if (totalData != null) {
                totalData.setValue(data);
            }
            //finalize();
        }
    }

    protected void completeSubOperation(SubOperation subOp) {
        switch (subOp.subOperationType) {
            case INGEST_SUB_OP:
                data += flushUnit.maxCapacities[flushUnit.currentComponentIndex];
                flushUnit.setMemoryComponentFull();
                if (isFlushable(flushUnit)) {
                    scheduleFlushOperation(flushUnit);
                }
                break;
            case FLUSH_SUB_OP:
                // flush completed
                if (flushUnit.operation.completeSubOperation()) {
                    completeFlushOperation(flushUnit);
                } else {
                    flushUnit.operation.initializeNewSubOperation();
                }
                break;
            default:
                int level = subOp.subOperationType - MERGE_SUB_OP_START;
                if (mergeUnits[level].operation.completeSubOperation()) {
                    completeMergeOperation(mergeUnits[level]);
                } else {
                    mergeUnits[level].operation.initializeNewSubOperation();
                }
                break;
        }
    }

    protected boolean isFlushable(FlushUnit flushUnit) {
        return flushUnit.operation == null && flushUnit.numUsedCurrentComponents > 0;
    }

    protected void operationCompleted() {
        numRunningOperations--;
        assert numRunningOperations >= 0 && numRunningOperations <= numLevels + 1;
    }

    protected void operationScheduled() {
        numRunningOperations++;
        assert numRunningOperations >= 0;
    }

    public void scheduleFlushOperation(FlushUnit flushUnit) {
        assert flushUnit.operation == null;
        operationScheduled();
        IoOperation flushOp = initializeFlushOperation(flushUnit.maxCapacities[flushUnit.currentFlushIndex]);
        flushUnit.operation = flushOp;
        addNewIoOperation(flushOp);
    }

    public void completeFlushOperation(FlushUnit flushUnit) {
        FlushOperation flushOp = flushUnit.getOperation();
        if (flushOp.isCompleted) {
            return;
        }
        flushOp.setCompleted();
        operationCompleted();
        assert numRunningOperations >= 0;
        if (addToMergeUnit(mergeUnits[0], flushOp, flushUnit)) {
            finalizeFlushOperation(flushUnit);
        }
    }

    protected void finalizeFlushOperation(FlushUnit flushUnit) {
        if (flushUnit.numUsedCurrentComponents == flushUnit.maxCapacities.length) {
            // we need to active a new component
            flushUnit.activeNextMemoryComponent();
        } else {
            flushUnit.maxCapacities[flushUnit.currentFlushIndex] = 0;
        }
        flushUnit.numUsedCurrentComponents--;
        flushUnit.currentFlushIndex = (flushUnit.currentFlushIndex + 1) % flushUnit.maxCapacities.length;
        flushUnit.operation = null;
        if (isFlushable(flushUnit)) {
            scheduleFlushOperation(flushUnit);
        }
    }

    protected boolean addToMergeUnit(MergeUnit mergeUnit, IoOperation op, Unit sourceUnit) {
        if (mergeUnit.isFull()) {
            mergeUnit.setBlockedUnit(sourceUnit);
            return false;
        } else {
            assert op.numOutputComponents == 1;
            mergeUnit.addComponent(op.outputComponents[0]);
            op.numOutputComponents = 0;
            if (isMergeable(mergeUnit)) {
                scheduleMergeOperation(mergeUnit);
            }
            return true;
        }
    }

    public MergeOperation scheduleMergeOperation(MergeUnit mergeUnit) {
        checkMergeFeasibility(mergeUnit.level);
        MergeOperation mergeOp = doScheduleMergeOperation(mergeUnit);
        mergeUnit.operation = mergeOp;
        if (mergeOp != null) {
            TOTAL_MERGES[mergeUnit.level]++;
            TOTAL_MERGE_CURRENT_LEVEL_COSTS[mergeUnit.level] += mergeOp.numComponentsInCurrentLevel;
            TOTAL_MERGE_NEXT_LEVEL_COSTS[mergeUnit.level] += mergeOp.numComponentsInNextLevel;
            operationScheduled();
            addNewIoOperation(mergeOp);
        }
        return mergeOp;
    }

    protected abstract MergeOperation doScheduleMergeOperation(MergeUnit mergeUnit);

    public void addNewSubOperation(SubOperation subOp) {
        newSubOperations[numNewSubOperations++] = subOp;
    }

    public void addNewIoOperation(IoOperation ioOp) {
        if (ioOp instanceof MergeOperation) {
            runningMerges++;
        }
        newIoOperations[numNewIoOperations++] = ioOp;
    }

    public void prepare(double ingestSpeed, double[] memoryComponentCapacities, double currentMemoryComponentCapacity,
            double flushedCapacity, double flusheFinalizedPages, double flushSubOperationElapsedTime,
            double[][] mergeComponents, double[] mergedCapacities, double[] mergeFinalizedPages,
            double[] mergeSubOperationElapsedTimes) {
        for (int i = 0; i < TOTAL_MERGES.length; i++) {
            TOTAL_MERGES[i] = 0;
            TOTAL_MERGE_CURRENT_LEVEL_COSTS[i] = 0;
            TOTAL_MERGE_NEXT_LEVEL_COSTS[i] = 0;
        }
        opQueue.clear();
        numRunningOperations = 0;
        numNewIoOperations = 0;
        numNewSubOperations = 0;

        flushUnit.initialize(memoryComponentCapacities, currentMemoryComponentCapacity, ingestSpeed);
        if (isFlushable(flushUnit)) {
            scheduleFlushOperation(flushUnit);
            flushUnit.operation.initialize(flushedCapacity, flusheFinalizedPages);
        }
        for (int i = 0; i < mergeUnits.length; i++) {
            Component[] components = new Component[mergeComponents[i].length];
            for (int j = 0; j < components.length; j++) {
                components[j] = Component.get();
                components[j].initialize(mergeComponents[i][j], 0);
            }
            mergeUnits[i].initialize(components);
        }

        for (int i = 0; i < mergeUnits.length; i++) {
            if (isMergeable(mergeUnits[i])) {
                scheduleMergeOperation(mergeUnits[i]);
                mergeUnits[i].operation.initialize(mergedCapacities[i], mergeFinalizedPages[i]);
            }
        }

        numNewIoOperations = 0;
        numNewSubOperations = 0;

        // activate sub operations
        if (flushUnit.operation != null) {
            if (!flushUnit.operation.finishedProcessing()) {
                flushUnit.operation.initializeNewSubOperation();
                flushUnit.operation.subOperation.remainingTime -= flushSubOperationElapsedTime;
            }
        }

        for (int i = 0; i < mergeUnits.length; i++) {
            MergeOperation mergeOp = mergeUnits[i].getOperation();
            if (mergeOp != null && !mergeOp.finishedProcessing()) {
                mergeOp.initializeNewSubOperation();
                mergeOp.subOperation.remainingTime -= mergeSubOperationElapsedTimes[i];
            }
        }

        numNewIoOperations = 0;
        numNewSubOperations = 0;
        // examine flush/merge operations have completed or not
        // activate sub operations
        if (flushUnit.operation != null) {
            FlushOperation flushOp = flushUnit.getOperation();
            if (!flushOp.finishedProcessing()) {
                if (flushOp.subOperation.active) {
                    if (flushOp.subOperation.isCompleted()) {
                        boolean flushCompleted = flushOp.completeSubOperation();
                        if (flushCompleted) {
                            completeFlushOperation(flushUnit);
                        } else {
                            addNewIoOperation(flushOp);
                        }
                    } else {
                        // current op is still active
                        opQueue.add(flushOp.subOperation);
                    }
                }
            } else {
                completeFlushOperation(flushUnit);
            }
        }

        for (int i = 0; i < mergeUnits.length; i++) {
            MergeOperation mergeOp = mergeUnits[i].getOperation();
            if (mergeOp == null) {
                continue;
            }

            if (!mergeOp.finishedProcessing()) {
                if (mergeOp.subOperation.active) {
                    if (mergeOp.subOperation.isCompleted()) {
                        boolean mergeCompleted = mergeOp.completeSubOperation();
                        if (mergeCompleted) {
                            completeMergeOperation(mergeUnits[i]);
                        } else {
                            addNewIoOperation(mergeOp);
                        }
                    } else {
                        // current op is still active
                        opQueue.add(mergeOp.subOperation);
                    }
                }
            } else {
                completeMergeOperation(mergeUnits[i]);
            }

        }
        numNewSubOperations = 0;
        for (int i = 0; i < numNewIoOperations; i++) {
            // activate newly scheduled io operations
            newIoOperations[i].initializeNewSubOperation();
        }
        numNewIoOperations = 0;
        for (int i = 0; i < numNewSubOperations; i++) {
            opQueue.add(newSubOperations[i]);
        }
        numNewSubOperations = 0;

        if (!flushUnit.isFull()) {
            opQueue.add(flushUnit.ingestOp);
        }
    }

    protected void finalize() {
        time = 0;
        unpinComponents(flushOperation.outputComponents, flushOperation.numOutputComponents);
        flushOperation.numOutputComponents = 0;
        for (int i = 0; i < numLevels; i++) {
            MergeOperation mergeOp = mergeOperations[i];
            unpinComponents(mergeOp.outputComponents, mergeOp.numOutputComponents);
            mergeOp.numOutputComponents = 0;

            MergeUnit unit = mergeUnits[i];
            unpinComponents(unit.components, unit.numComponents);
            unit.numComponents = 0;
        }
    }

    protected void unpinComponents(Component[] components, int numComponents) {
        for (int i = 0; i < numComponents; i++) {
            components[i].unpin();
        }
    }

    protected void checkMergeFeasibility(int level) {
        if (level >= numLevels) {
            throw new SimulateTerminateException(level);
        }
    }

    protected FlushOperation initializeFlushOperation(double totalCapacity) {
        flushOperation.reset(totalCapacity, pageEstimator.estiamtePages(totalCapacity));
        Component outputComponent = Component.get();
        outputComponent.initialize(totalCapacity, 0);
        flushOperation.outputComponents[0] = outputComponent;
        flushOperation.numOutputComponents = 1;
        return flushOperation;
    }

    protected abstract MergeOperation createMergeOperaiton(int level, ISpeedProvider speedProvider,
            SubOperation subOperation);

    protected abstract MergeUnit createMergeUnit(int level);

    protected abstract boolean isMergeable(MergeUnit unit);

    protected void completeMergeOperation(MergeUnit unit) {
        runningMerges--;
    }

    public double getSubOperationPages() {
        return subOperationPages;
    }

    public double getSubOperationProcessingRecords() {
        return subOperationProcessingRecords;
    }

    public int getNumRunningOperations() {
        return numRunningOperations;
    }

}
