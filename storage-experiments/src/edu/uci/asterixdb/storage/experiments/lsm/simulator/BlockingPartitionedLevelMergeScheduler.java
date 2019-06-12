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

public class BlockingPartitionedLevelMergeScheduler extends AbstractPartitionedLevelMergeScheduler {
    private static final Logger LOGGER = LogManager.getLogger(BlockingPartitionedLevelMergeScheduler.class);

    public BlockingPartitionedLevelMergeScheduler(int toleratedComponentsPerLevel, int numLevels,
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
        } else if (level == 1) {
            int maxNumComponentsLevel1 = (int) Math.ceil(baseLevelCapacity / diskComponentCapacity);
            if (DEBUG) {
                LOGGER.error("Level {} has maximum number of components {}", level, maxNumComponentsLevel1);
            }
            return new MergeUnit(level, maxNumComponentsLevel1, sizeRatioLevel0 + 1, 0 /* base capacity not used */);
        } else {
            int maxNumComponentsLevel1 = (int) Math.ceil(baseLevelCapacity / diskComponentCapacity);
            int maxNumComponents = maxNumComponentsLevel1 * (int) Math.pow(sizeRatio, level - 1);
            if (DEBUG) {
                LOGGER.error("Level {} has maximum number of components {}", level, maxNumComponents);
            }
            return new MergeUnit(level, maxNumComponents, EXTRA_COMPONENTS, 0 /* base capacity not used */);
        }
    }

    @Override
    protected MergeOperation createMergeOperaiton(int level, ISpeedProvider speedProvider, SubOperation subOperation) {
        if (level == 0) {
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, sizeRatioLevel0,
                    mergeUnits[1].maxNumComponents, sizeRatioLevel0 * 2 + mergeUnits[1].maxNumComponents);
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
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, 1, sizeRatio * 3,
                    sizeRatio * 3 + 1);
            op.numComponentsInCurrentLevel = 1;
            op.numComponentsInNextLevel = 0;
            return op;
        }
    }

    @Override
    public boolean isMergeable(MergeUnit mergeUnit) {
        if (mergeUnit.operation != null || runningMerges > 0) {
            return false;
        }

        if (mergeUnit.level == 0) {
            // for level 0
            if (mergeUnit.numComponents >= sizeRatioLevel0) {
                MergeUnit nextUnit = mergeUnits[1];
                if (nextUnit.numComponents <= nextUnit.maxNumComponents) {
                    return true;
                } else {
                    if (DEBUG) {
                        LOGGER.error("{}: Cannot schedule merge in level 0, set blocked", time);
                    }
                    nextUnit.setBlockedUnit(mergeUnit);
                }
            }
            return false;
        } else {
            return mergeUnit.numComponents > mergeUnit.maxNumComponents;
        }
    }

    @Override
    protected MergeOperation doScheduleMergeOperation(MergeUnit mergeUnit) {
        MergeOperation mergeOp = mergeOperations[mergeUnit.level];
        if (mergeUnit.level == 0) {
            assert mergeUnit.numComponents >= sizeRatioLevel0;
            // merge multiple components in level 0 into next level components
            double totalCapacity = 0;
            Component sumComponent = Component.get();
            for (int i = 0; i < sizeRatioLevel0; i++) {
                totalCapacity += mergeUnit.components[i].records;
                mergeOp.componentsInCurrentLevel[i] = mergeUnit.components[i];
            }
            int minLSN = getMinLSN(mergeUnit.components, mergeUnit.numComponents);
            sumComponent.reset(MIN_RANGE, MAX_RANGE, totalCapacity, minLSN);
            mergeOp.numComponentsInCurrentLevel = sizeRatioLevel0;
            Component[] sumComponents = new Component[] { sumComponent };
            if (!selectMergeComponents(mergeUnit.level, mergeOp, sumComponents, 1, mergeUnits[1])) {
                // Cannot schedule the merge for level 0, block
                sumComponent.unpin();
                mergeUnits[1].setBlockedUnit(mergeUnit);
                return null;
            }

            for (int i = 0; i < mergeOp.numComponentsInNextLevel; i++) {
                totalCapacity += mergeOp.componentsInNextLevel[i].records;
            }
            double resultRatio = 1.0;
            //double resultRatio = LSMFlowControlUtils.nextGaussian(mergeComponentRatios[0][0]);
            double resultCapacity = totalCapacity * resultRatio;
            mergeOp.reset(resultCapacity, pageEstimator.estiamtePages(resultCapacity));

            // build the output component
            if (mergeOp.numComponentsInNextLevel > 0) {
                mergeOp.numOutputComponents = buildMergeOutputComponents(sumComponents, 1,
                        mergeOp.componentsInNextLevel, mergeOp.numComponentsInNextLevel, mergeOp.outputComponents);
            } else {
                mergeOp.numOutputComponents = buildMergeOutputComponentsForLevel0(mergeOp.componentsInCurrentLevel,
                        mergeOp.numComponentsInCurrentLevel, totalCapacity, mergeOp.outputComponents);
            }
            for (int i = 0; i < mergeOp.numComponentsInNextLevel; i++) {
                mergeOp.componentsInNextLevel[i].isMerging = true;
            }
            sumComponent.unpin();
            if (DEBUG) {
                LOGGER.error("{}: Schedule merge level 0 components {}.", time,
                        toString(mergeOp.componentsInCurrentLevel, mergeOp.numComponentsInCurrentLevel));
            }
            return mergeOp;
        }
        while (isMergeable(mergeUnit)) {
            if (mergeUnit.level + 1 < numLevels) {
                MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
                // select the component
                if (!selectMergeComponents(mergeUnit.level, mergeOp, mergeUnit.components, mergeUnit.numComponents,
                        nextUnit)) {
                    if (DEBUG) {
                        LOGGER.error("{}: Cannot schedule merge for components {} in level {}", time,
                                toString(mergeUnit.components, mergeUnit.numComponents), mergeUnit.level);
                    }
                    nextUnit.setBlockedUnit(mergeUnit);
                    return null;
                }
                if (mergeOp.numComponentsInNextLevel == 0) {
                    // no need to do merge, we simply push the component into the next level
                    if (nextUnit.numComponents < nextUnit.maxNumComponents + nextUnit.extraNumComponents) {
                        mergeOp.componentsInCurrentLevel[0].pin();
                        addComponent(nextUnit, mergeOp.componentsInCurrentLevel[0]);
                        assert sanityCheck(nextUnit.components, nextUnit.numComponents);
                        removeComponent(mergeUnit, mergeOp.componentsInCurrentLevel[0]);
                        if (DEBUG) {
                            LOGGER.error("{}: Push a level {} component {} to level {}", time, mergeUnit.level,
                                    toString(mergeOp.componentsInCurrentLevel, 1), mergeUnit.level + 1);
                        }
                        notifyBlockedUnit(mergeUnit);
                        if (isMergeable(nextUnit)) {
                            scheduleMergeOperation(nextUnit);
                        }
                    } else {
                        // cannot push
                        nextUnit.setBlockedUnit(mergeUnit);
                        return null;
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
                    int index = getNextComponentIndex(mergeUnit.components, mergeUnit.numComponents,
                            lastMergeKeys[mergeUnit.level]);
                    if (!mergeUnit.components[index].isMerging) {
                        LOGGER.error("{}: Remove component {} in level {} with min LSN {}", time,
                                mergeUnit.components[index], mergeUnit.level, mergeUnit.components[index].LSN);
                        lastMergeKeys[mergeUnit.level] = mergeUnit.components[index].max;
                        removeComponent(mergeUnit, mergeUnit.components[index]);
                        assert sanityCheck(mergeUnit.components, mergeUnit.numComponents);
                    } else {
                        break;
                    }
                }
                notifyBlockedUnit(mergeUnit);
                return null;
            }
        }
        return null;
    }

    private void finalizeMergeOperationInLevel0(MergeUnit mergeUnit) {
        MergeOperation op = mergeUnit.getOperation();
        assert op.numComponentsInCurrentLevel == sizeRatioLevel0;
        assert op.componentsInCurrentLevel[0] != mergeUnit.components[0];
        MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
        for (int i = 0; i < sizeRatioLevel0; i++) {
            mergeUnit.components[i].unpin();
        }
        for (int i = 0; i < mergeUnit.numComponents - sizeRatioLevel0; i++) {
            mergeUnit.components[i] = mergeUnit.components[i + sizeRatioLevel0];
        }
        mergeUnit.numComponents -= sizeRatioLevel0;

        if (op.numComponentsInNextLevel > 0) {
            replaceComponents(nextUnit, op.componentsInNextLevel, op.numComponentsInNextLevel, op.outputComponents,
                    op.numOutputComponents);
        } else {
            for (int i = 0; i < op.numOutputComponents; i++) {
                nextUnit.components[i] = op.outputComponents[i];
            }
            nextUnit.numComponents = op.numOutputComponents;
        }
        mergeUnit.operation = null;
        if (mergeUnit.blockedUnit != null) {
            FlushUnit flushUnit = (FlushUnit) mergeUnit.blockedUnit;
            mergeUnit.unsetBlockedUnit();
            mergeUnit.addComponent(flushUnit.getOperation().outputComponents[0]);
            finalizeFlushOperation(flushUnit);
        }

        for (int i = 0; i < numLevels; i++) {
            if (isMergeable(mergeUnits[i])) {
                scheduleMergeOperation(mergeUnits[i]);
            }
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

        notifyBlockedUnit(mergeUnit);
        for (int i = numLevels - 1; i >= 0; i--) {
            if (isMergeable(mergeUnits[i])) {
                scheduleMergeOperation(mergeUnits[i]);
            }
        }
    }

    protected void completeMergeOperation(MergeUnit mergeUnit) {
        super.completeMergeOperation(mergeUnit);
        operationCompleted();
        mergeUnit.operation.setCompleted();
        if (DEBUG) {
            MergeOperation mergeOp = (MergeOperation) mergeUnit.operation;
            LOGGER.error(
                    "{}: Complete merge in level {} with input components: {} and {} with {} output components: {}",
                    time, mergeUnit.level,
                    toString(mergeOp.componentsInCurrentLevel, mergeOp.numComponentsInCurrentLevel),
                    toString(mergeOp.componentsInNextLevel, mergeOp.numComponentsInNextLevel),
                    mergeUnit.operation.numOutputComponents,
                    toString(mergeUnit.operation.outputComponents, mergeUnit.operation.numOutputComponents));
        }
        if (mergeUnit.level == 0) {
            finalizeMergeOperationInLevel0(mergeUnit);
        } else {
            MergeOperation op = mergeUnit.getOperation();
            MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
            if (nextUnit.numComponents + op.numOutputComponents
                    - op.numComponentsInNextLevel <= nextUnit.maxNumComponents + nextUnit.extraNumComponents) {
                // we can proceed now
                finalizeMergeOperation(mergeUnit);
            } else {
                nextUnit.setBlockedUnit(mergeUnit);
            }
        }
    }

    private void notifyBlockedUnit(MergeUnit mergeUnit) {
        if (mergeUnit.blockedUnit != null) {
            MergeUnit blockedUnit = (MergeUnit) mergeUnit.blockedUnit;
            if (mergeUnit.level == 1) {
                assert blockedUnit.operation == null;
                if (isMergeable(blockedUnit)) {
                    mergeUnit.unsetBlockedUnit();
                    MergeOperation newMergeOp = scheduleMergeOperation(blockedUnit);
                    if (newMergeOp == null) {
                        scheduleMergeOperation(blockedUnit);
                    }
                    assert newMergeOp != null;
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
    }

}
