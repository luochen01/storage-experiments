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

public class TierMergeScheduler extends AbstractOperationScheduler {

    private final RandomVariable[][] mergeComponentRatios;

    public TierMergeScheduler(int toleratedComponentsPerLevel, int numLevels, RandomVariable memoryComponentCapacity,
            int totalMemoryComponents, int sizeRatio, ISpeedProvider flushSpeed, ISpeedProvider[] mergeSpeeds,
            ILSMFinalizingPagesEstimator pageEstimator, double subOperationProcessingRecords, double subOperationPages,
            double baseLevelCapacity, RandomVariable[][] mergeComponentRatios) {
        super(toleratedComponentsPerLevel, numLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio,
                flushSpeed, mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                baseLevelCapacity);
        this.mergeComponentRatios = mergeComponentRatios;
    }

    @Override
    public boolean isMergeable(MergeUnit mergeUnit) {
        return mergeUnit.numComponents >= sizeRatio && mergeUnit.operation == null;
    }

    @Override
    protected MergeOperation doScheduleMergeOperation(MergeUnit mergeUnit) {
        assert mergeUnit.numComponents >= sizeRatio;
        MergeOperation op = mergeOperations[mergeUnit.level];
        double resultRatio = getMergeResultRatio(mergeUnit.level);
        assert resultRatio > 0 && resultRatio <= 1;
        double total = 0;
        for (int i = 0; i < sizeRatio; i++) {
            total += mergeUnit.components[i].records;
        }

        Component outputComponent = Component.get();
        outputComponent.initialize(total * resultRatio);
        op.outputComponents[0] = outputComponent;
        op.numOutputComponents = 1;
        op.reset(total * resultRatio, pageEstimator.estiamtePages(total * resultRatio));
        System.arraycopy(mergeUnit.components, 0, op.componentsInCurrentLevel, 0, sizeRatio);
        return op;
    }

    protected void completeMergeOperation(MergeUnit mergeUnit) {
        MergeOperation op = mergeUnit.getOperation();
        if (op.isCompleted) {
            return;
        }
        op.setCompleted();
        operationCompleted();
        assert numRunningOperations >= 0;
        // move components to the next level
        if (mergeUnit.level + 1 < numLevels) {
            MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
            if (addToMergeUnit(nextUnit, op, mergeUnit)) {
                finalizeMergeOperation(mergeUnit, true);
            }
        } else {
            op.outputComponents[0].unpin();
            op.numOutputComponents = 0;
            finalizeMergeOperation(mergeUnit, true);
        }
    }

    private void finalizeMergeOperation(MergeUnit mergeUnit, boolean removeOldComponents) {
        if (removeOldComponents) {
            for (int i = 0; i < sizeRatio; i++) {
                mergeUnit.components[i].unpin();
            }
            int remainingComponents = mergeUnit.numComponents - sizeRatio;
            for (int i = 0; i < remainingComponents; i++) {
                mergeUnit.components[i] = mergeUnit.components[i + sizeRatio];
                mergeUnit.components[i + sizeRatio] = null;
            }
            mergeUnit.numComponents = remainingComponents;
            assert mergeUnit.numComponents >= 0;
        }
        if (mergeUnit.blockedUnit != null) {
            Unit blockedUnit = mergeUnit.blockedUnit;
            mergeUnit.unsetBlockedUnit();
            addToMergeUnit(mergeUnit, blockedUnit.operation, blockedUnit);
            if (mergeUnit.level == 0) {
                finalizeFlushOperation((FlushUnit) blockedUnit);
            } else {
                finalizeMergeOperation((MergeUnit) blockedUnit, true);
            }
        }
        mergeUnit.operation = null;
        if (isMergeable(mergeUnit)) {
            scheduleMergeOperation(mergeUnit);
        }
    }

    private double getMergeResultRatio(int level) {
        if (mergeComponentRatios == null) {
            return 1;
        }
        double ratio = mergeComponentRatios[level][0].mean;
        return DoubleUtil.equals(ratio, 0) ? 1 : ratio;
    }

    @Override
    protected MergeOperation createMergeOperaiton(int level, ISpeedProvider speedProvider, SubOperation subOperation) {
        MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, sizeRatio, 0, 1);
        op.numComponentsInCurrentLevel = sizeRatio;
        op.numComponentsInNextLevel = 0;
        return op;
    }

    @Override
    protected MergeUnit createMergeUnit(int level) {
        return new MergeUnit(level, sizeRatio + toleratedComponentsPerLevel, 0, 0);
    }

}
