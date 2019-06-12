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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LevelMergeScheduler extends AbstractOperationScheduler {

    private static final Logger LOGGER = LogManager.getLogger(LevelMergeScheduler.class);
    private final RandomVariable[][] mergeComponentRatios;

    public LevelMergeScheduler(int toleratedComponentsPerLevel, int numLevels, RandomVariable memoryComponentCapacity,
            int totalMemoryComponents, int sizeRatio, ISpeedProvider flushSpeed, ISpeedProvider[] mergeSpeeds,
            ILSMFinalizingPagesEstimator pageEstimator, double subOperationProcessingRecords, double subOperationPages,
            double baseLevelCapacity, RandomVariable[][] mergeComponentRatios) {
        super(toleratedComponentsPerLevel, numLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio,
                flushSpeed, mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                baseLevelCapacity);
        this.mergeComponentRatios = mergeComponentRatios;
        assert toleratedComponentsPerLevel >= 1;
    }

    @Override
    public boolean isMergeable(MergeUnit mergeUnit) {
        if (mergeUnit.operation != null || mergeUnit.numComponents == 0 || mergeUnit.level + 1 == numLevels) {
            return false;
        }
        if (mergeUnit.level > 0 && !mergeUnit.components[0].isFull) {
            return false;
        }
        MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
        return !nextUnit.isFull() || !nextUnit.lastComponent().isFull;
    }

    @Override
    protected MergeOperation doScheduleMergeOperation(MergeUnit mergeUnit) {
        Component component = mergeUnit.components[0];
        MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
        if (nextUnit.numComponents == 0 || nextUnit.lastComponent().isFull) {
            // we should start with a new component
            assert !nextUnit.isFull();
            component.pin();
            component.isFull = false;
            nextUnit.addComponent(component);
            mergeUnit.removeComponent(0);
            return null;
        } else {
            // we should schedule a merge
            Component nextComponent = nextUnit.lastComponent();
            MergeOperation mergeOp = mergeOperations[mergeUnit.level];
            mergeOp.componentsInCurrentLevel[0] = component;
            mergeOp.componentsInNextLevel[0] = nextComponent;
            Component outputComponent = Component.get();
            double totalRecords = component.records + nextComponent.records;
            // TODO: improve merge result ratio
            double sizeRatio = getMergeResultRatio(mergeUnit.level, nextComponent.records, component.records);
            outputComponent.initialize(totalRecords * sizeRatio, 0);
            mergeOp.outputComponents[0] = outputComponent;
            mergeOp.numOutputComponents = 1;

            mergeOp.reset(totalRecords * sizeRatio, pageEstimator.estiamtePages(totalRecords * sizeRatio));

            return mergeOp;
        }
    }

    protected void completeMergeOperation(MergeUnit mergeUnit) {
        MergeOperation op = mergeUnit.getOperation();
        mergeUnit.operation = null;
        //        System.out.println(String.format("time: %.1f, complete merge in level %d. component %.0f + %.0f", time,
        //                op.level, op.componentsInCurrentLevel[0].records, op.componentsInNextLevel[0].records));

        operationCompleted();

        assert op.componentsInCurrentLevel[0] == mergeUnit.components[0];
        // we always merge 0th component of the current merge unit
        mergeUnit.removeComponent(0);

        MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
        assert nextUnit.numComponents > 0;
        assert op.componentsInNextLevel[0] == nextUnit.components[nextUnit.numComponents - 1];
        // update components in the next level
        nextUnit.components[nextUnit.numComponents - 1].unpin();
        assert op.numOutputComponents == 1;
        nextUnit.components[nextUnit.numComponents - 1] = op.outputComponents[0];
        op.numOutputComponents = 0;

        assert op.totalCapacity > 0;
        int actualSizeRatio = DoubleUtil.getMultiplier(op.totalCapacity, nextUnit.baseCapacity);

        if (actualSizeRatio >= sizeRatio) {
            // mark the latest component as full
            nextUnit.components[nextUnit.numComponents - 1].isFull = true;
            if (nextUnit.level + 1 < numLevels) {
                // we schedule a merge operation for the next unit
                if (isMergeable(nextUnit)) {
                    scheduleMergeOperation(nextUnit);
                }
            } else {
                // we simply reset num components to 0
                nextUnit.removeComponent(0);
                assert nextUnit.numComponents == 0;
            }
        }

        if (mergeUnit.level == 0) {
            // we should complete blocked flush operation
            FlushUnit flushUnit = (FlushUnit) mergeUnit.blockedUnit;
            if (flushUnit != null) {
                mergeUnit.unsetBlockedUnit();
                addToMergeUnit(mergeUnit, flushUnit.operation, flushUnit);
            }
        } else {
            // schedule new merge operations for the previous level
            if (isMergeable(mergeUnit)) {
                scheduleMergeOperation(mergeUnit);
            }
        }
    }

    private double getMergeResultRatio(int level, double largeSize, double smallSize) {
        if (mergeComponentRatios == null) {
            return 1;
        }
        int multiplier = DoubleUtil.getMultiplier(largeSize, smallSize);
        assert level >= 0;
        assert multiplier >= 1;
        assert multiplier <= sizeRatio;
        double ratio = DoubleUtil.nextGaussian(mergeComponentRatios[level][multiplier]);
        return DoubleUtil.equals(ratio, 0) ? 1 : ratio;
    }

    @Override
    protected MergeUnit createMergeUnit(int level) {
        return new MergeUnit(level, 1 + toleratedComponentsPerLevel, 0,
                Math.pow(sizeRatio, level - 1) * baseLevelCapacity);
    }

    @Override
    protected MergeOperation createMergeOperaiton(int level, ISpeedProvider speedProvider, SubOperation subOperation) {
        MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, 1, 1, 1);
        op.numComponentsInCurrentLevel = 1;
        op.numComponentsInNextLevel = 1;
        return op;
    }

}
