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

public class NonBlockingPartitionedLevelMergeScheduler extends AbstractPartitionedLevelMergeScheduler {
    private static final Logger LOGGER = LogManager.getLogger(NonBlockingPartitionedLevelMergeScheduler.class);

    public NonBlockingPartitionedLevelMergeScheduler(int toleratedComponentsPerLevel, int numLevels,
            RandomVariable memoryComponentCapacity, int totalMemoryComponents, int sizeRatio, ISpeedProvider flushSpeed,
            ISpeedProvider[] mergeSpeeds, ILSMFinalizingPagesEstimator pageEstimator,
            double subOperationProcessingRecords, double subOperationPages, double baseLevelCapacity,
            RandomVariable[][] mergeComponentRatios, int sizeRatioLevel0, int toleratedComponentsLevel0,
            double diskComponentCapacity) {
        super(toleratedComponentsPerLevel, numLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio,
                flushSpeed, mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                diskComponentCapacity);
    }

    @Override
    protected MergeUnit createMergeUnit(int level) {
        if (level == 0) {
            return new MergeUnit(0, sizeRatioLevel0 + toleratedComponentsLevel0, 0, 0 /*base capacity not used*/);
        } else {
            // TODO: do we need to fix this?
            int maxNumComponentsLevel1 = (int) Math.ceil(baseLevelCapacity / diskComponentCapacity);
            int maxNumComponents = maxNumComponentsLevel1 * (int) Math.pow(sizeRatio, level - 1);
            if (DEBUG) {
                LOGGER.error("Level {} has maximum number of components {}", level, maxNumComponents);
            }
            return new MergeUnit(level, maxNumComponents, level == 1 ? maxNumComponents : EXTRA_COMPONENTS,
                    0 /* base capacity not used */);
        }
    }

    @Override
    protected MergeOperation createMergeOperaiton(int level, ISpeedProvider speedProvider, SubOperation subOperation) {
        if (level == 0) {
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, sizeRatioLevel0, 0,
                    (int) Math.ceil(sizeRatioLevel0 * memoryComponentCapacity.max / diskComponentCapacity));
            op.numComponentsInCurrentLevel = sizeRatioLevel0;
            op.numComponentsInNextLevel = 0;
            return op;
        } else if (level == 1) {
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, 1,
                    sizeRatio * sizeRatio, sizeRatio * sizeRatio + 1);
            op.numComponentsInCurrentLevel = 1;
            op.numComponentsInNextLevel = 0;
            return op;
        } else {
            MergeOperation op =
                    new MergeOperation(level, 0, subOperation, this, speedProvider, 1, sizeRatio, sizeRatio + 1);
            op.numComponentsInCurrentLevel = 1;
            op.numComponentsInNextLevel = 0;
            return op;
        }
    }

    @Override
    public boolean isMergeable(MergeUnit mergeUnit) {
        if (mergeUnit.operation != null) {
            return false;
        }
        if (mergeUnit.level == 0) {
            // for level 0
            return mergeUnit.numComponents >= sizeRatioLevel0;
        } else {
            if (mergeUnit.level == 1) {
                return mergeUnit.numComponents > 0;
            } else {
                return mergeUnit.numComponents >= mergeUnit.maxNumComponents;
            }
        }
    }

    @Override
    protected MergeOperation doScheduleMergeOperation(MergeUnit mergeUnit) {
        MergeOperation mergeOp = mergeOperations[mergeUnit.level];
        if (mergeUnit.level == 0) {
            assert mergeUnit.numComponents >= sizeRatioLevel0;
            // merge multiple components in level 0 into next level components
            double totalCapacity = 0;
            for (int i = 0; i < sizeRatioLevel0; i++) {
                mergeOp.componentsInCurrentLevel[i] = mergeUnit.components[i];
                totalCapacity += mergeUnit.components[i].records;
            }
            double resultRatio = DoubleUtil.nextGaussian(mergeComponentRatios[0][0]);
            double resultCapacity = totalCapacity * resultRatio;
            mergeOp.reset(resultCapacity, pageEstimator.estiamtePages(resultCapacity));
            // figure out the output components here
            mergeOp.numOutputComponents = buildMergeOutputComponentsForLevel0(mergeOp.componentsInCurrentLevel,
                    mergeOp.numComponentsInCurrentLevel, mergeOp.totalCapacity, mergeOp.outputComponents);
            if (DEBUG) {
                LOGGER.error("{}: Schedule merge in level 0 components {} into {}. operations {}", time,
                        toString(mergeOp.componentsInCurrentLevel, mergeOp.numComponentsInCurrentLevel),
                        toString(mergeOp.outputComponents, mergeOp.numOutputComponents), numRunningOperations + 1);
            }
            return mergeOp;
        }
        while (isMergeable(mergeUnit)) {
            if (mergeUnit.level + 1 < numLevels) {
                MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
                // select the component
                if (!selectMergeComponents(mergeOp, mergeUnit.components, mergeUnit.numComponents, nextUnit, true)) {
                    if (DEBUG) {
                        LOGGER.error("{}: Cannot schedule merge for components {} in level {}", time,
                                toString(mergeUnit.components, mergeUnit.numComponents), mergeUnit.level);
                    }
                    nextUnit.setBlockedUnit(mergeUnit);
                    return null;
                }
                if (mergeOp.numComponentsInNextLevel == 0) {
                    // no need to do merge, we simply push the component into the next level
                    mergeOp.componentsInCurrentLevel[0].pin();
                    addComponent(nextUnit, mergeOp.componentsInCurrentLevel[0]);
                    assert sanityCheck(nextUnit.components, nextUnit.numComponents);
                    removeComponent(mergeUnit, mergeOp.componentsInCurrentLevel[0]);
                    if (DEBUG) {
                        LOGGER.error("{}: Push a level {} component {} to level {}", time, mergeUnit.level,
                                toString(mergeOp.componentsInCurrentLevel, 1), mergeUnit.level + 1);
                    }
                    // see if we can schedule more merges at this level
                } else {
                    mergeOp.componentsInCurrentLevel[0].isMerging = true;
                    double totalCapacity = mergeOp.componentsInCurrentLevel[0].records;
                    for (int i = 0; i < mergeOp.numComponentsInNextLevel; i++) {
                        mergeOp.componentsInNextLevel[i].isMerging = true;
                        totalCapacity += mergeOp.componentsInNextLevel[i].records;
                    }
                    double resultRatio = 1;
                    double resultCapacity = totalCapacity * resultRatio;
                    // build output components
                    mergeOp.reset(resultCapacity, pageEstimator.estiamtePages(resultCapacity));
                    mergeOp.numOutputComponents = buildMergeOutputComponents(mergeOp.componentsInCurrentLevel,
                            mergeOp.numComponentsInCurrentLevel, mergeOp.componentsInNextLevel,
                            mergeOp.numComponentsInNextLevel, mergeOp.outputComponents);
                    if (DEBUG) {
                        LOGGER.error(
                                "{}: Schedule merge level {} component {} with {} components {} in level {} into {}. operations {}",
                                time, mergeUnit.level,
                                toString(mergeOp.componentsInCurrentLevel, mergeOp.numComponentsInCurrentLevel),
                                mergeOp.numComponentsInNextLevel,
                                toString(mergeOp.componentsInNextLevel, mergeOp.numComponentsInNextLevel),
                                mergeUnit.level + 1, toString(mergeOp.outputComponents, mergeOp.numOutputComponents),
                                numRunningOperations + 1);
                    }
                    return mergeOp;
                }
            } else {
                // there is no next level, we simply select some components to drop (a round round fashion)
                while (mergeUnit.numComponents >= mergeUnit.maxNumComponents) {
                    if (!mergeUnit.components[mergeUnit.mergeIndex].isMerging) {
                        if (DEBUG) {
                            LOGGER.error("{}: Remove component {} in level {}, number of components {}", time,
                                    mergeUnit.components[mergeUnit.mergeIndex], mergeUnit.level,
                                    mergeUnit.numComponents);
                        }
                        removeComponent(mergeUnit, mergeUnit.components[mergeUnit.mergeIndex]);
                        assert sanityCheck(mergeUnit.components, mergeUnit.numComponents);

                    }
                    mergeUnit.mergeIndex = (mergeUnit.mergeIndex + 1) % mergeUnit.numComponents;
                }
                return null;
            }
        }
        return null;
    }

    private void finalizeMergeOperationInLevel0(MergeUnit mergeUnit) {
        MergeOperation op = mergeUnit.getOperation();
        MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
        for (int i = 0; i < op.numComponentsInCurrentLevel; i++) {
            mergeUnit.components[i].unpin();
        }
        for (int i = 0; i < mergeUnit.numComponents - op.numComponentsInCurrentLevel; i++) {
            mergeUnit.components[i] = mergeUnit.components[i + op.numComponentsInCurrentLevel];
        }
        mergeUnit.numComponents -= op.numComponentsInCurrentLevel;

        for (int i = 0; i < op.numOutputComponents; i++) {
            nextUnit.components[i] = op.outputComponents[i];
        }
        nextUnit.numComponents = op.numOutputComponents;
        mergeUnit.operation = null;
        if (mergeUnit.blockedUnit != null) {
            FlushUnit flushUnit = (FlushUnit) mergeUnit.blockedUnit;
            mergeUnit.unsetBlockedUnit();
            mergeUnit.addComponent(flushUnit.getOperation().outputComponents[0]);
            finalizeFlushOperation(flushUnit);
        }

        if (isMergeable(nextUnit)) {
            scheduleMergeOperation(nextUnit);
        }
        if (isMergeable(mergeUnit)) {
            scheduleMergeOperation(mergeUnit);
        }
    }

    private void finalizeMergeOperation(MergeUnit mergeUnit) {
        MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
        MergeOperation op = mergeUnit.getOperation();
        removeComponent(mergeUnit, op.componentsInCurrentLevel[0]);
        replaceComponents(nextUnit, op.componentsInNextLevel, op.numComponentsInNextLevel, op.outputComponents,
                op.numOutputComponents);
        assert sanityCheck(mergeUnit.components, mergeUnit.numComponents);
        assert sanityCheck(nextUnit.components, nextUnit.numComponents);
        mergeUnit.operation = null;
        if (mergeUnit.blockedUnit != null) {
            MergeUnit blockedUnit = (MergeUnit) mergeUnit.blockedUnit;
            if (mergeUnit.level == 1) {
                if (mergeUnit.numComponents == 0) {
                    mergeUnit.unsetBlockedUnit();
                    finalizeMergeOperationInLevel0(blockedUnit);
                }
            } else {
                // schedule a merge at blocked
                mergeUnit.unsetBlockedUnit();
                if (blockedUnit.operation != null) {
                    if (blockedUnit.operation.isCompleted) {
                        finalizeMergeOperation(blockedUnit);
                    }
                } else {
                    if (isMergeable(blockedUnit)) {
                        scheduleMergeOperation(blockedUnit);
                    }
                }
            }
        }

        if (isMergeable(nextUnit)) {
            scheduleMergeOperation(nextUnit);
        }
        // we should schedule more merges
        if (isMergeable(mergeUnit)) {
            scheduleMergeOperation(mergeUnit);
        }
    }

    protected void completeMergeOperation(MergeUnit mergeUnit) {
        operationCompleted();
        mergeUnit.operation.setCompleted();
        if (DEBUG) {
            LOGGER.error("{}: Complete merge in level {} with {} components. operations {}", time, mergeUnit.level,
                    mergeUnit.numComponents, numRunningOperations);
        }
        if (mergeUnit.level == 0) {
            MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
            if (nextUnit.numComponents != 0) {
                // we cannot proceed, have to wait now
                nextUnit.setBlockedUnit(mergeUnit);
            } else {
                finalizeMergeOperationInLevel0(mergeUnit);
            }
        } else {
            MergeOperation op = mergeUnit.getOperation();
            MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
            if (nextUnit.numComponents + op.numOutputComponents
                    - op.numComponentsInNextLevel < nextUnit.maxNumComponents + nextUnit.extraNumComponents) {
                // we can proceed now
                finalizeMergeOperation(mergeUnit);
            } else {
                nextUnit.setBlockedUnit(mergeUnit);
            }

        }

    }

}
