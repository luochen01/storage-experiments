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

class HybridMergeUnit extends MergeUnit {
    public final int[] numStackedComponents;
    public final Component[][] stackedComponents;
    public final double[] componentRanges;
    public int usedNumStacks;
    public final int maxNumStackedComponents;

    public HybridMergeUnit(int level, int maxNumComponents, int extraNumComponents, double baseCapacity,
            int maxNumStackedComponents) {
        super(level, maxNumComponents, extraNumComponents, baseCapacity);
        this.stackedComponents = new Component[maxNumComponents + extraNumComponents][maxNumComponents];
        this.numStackedComponents = new int[maxNumComponents + extraNumComponents];
        this.componentRanges = new double[maxNumComponents + extraNumComponents];
        this.maxNumStackedComponents = maxNumStackedComponents;
    }

    public int getCurrentMaxStackedComponents() {
        if (usedNumStacks == 0) {
            return 0;
        }
        int max = 0;
        for (int i = 0; i < usedNumStacks; i++) {
            max = Math.max(max, numStackedComponents[i]);
        }
        return max;
    }

    @Override
    public void initialize(Component[] components) {
        super.initialize(components);
        for (int i = 0; i < this.usedNumStacks; i++) {
            this.numStackedComponents[i] = 0;
        }
        this.usedNumStacks = 0;
    }

}

public class HybridPartitionedLevelMergeScheduler extends AbstractPartitionedLevelMergeScheduler {
    private static final Logger LOGGER = LogManager.getLogger(HybridPartitionedLevelMergeScheduler.class);
    private final int maxNumStackedComponents;

    public HybridPartitionedLevelMergeScheduler(int toleratedComponentsPerLevel, int numLevels,
            RandomVariable memoryComponentCapacity, int totalMemoryComponents, int sizeRatio, ISpeedProvider flushSpeed,
            ISpeedProvider[] mergeSpeeds, ILSMFinalizingPagesEstimator pageEstimator,
            double subOperationProcessingRecords, double subOperationPages, double baseLevelCapacity,
            RandomVariable[][] mergeComponentRatios, int sizeRatioLevel0, int toleratedComponentsLevel0,
            double diskComponentCapacity, int maxNumStackedComponents) {
        super(toleratedComponentsPerLevel, numLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio,
                flushSpeed, mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                diskComponentCapacity);
        this.maxNumStackedComponents = maxNumStackedComponents;
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
            if (level == 1) {
                return new HybridMergeUnit(level, maxNumComponents, EXTRA_COMPONENTS, 0, maxNumStackedComponents);
            } else if (level == 2) {
                return new MergeUnit(level, maxNumComponents, maxNumStackedComponents + EXTRA_COMPONENTS,
                        0 /* base capacity not used */);
            } else {
                return new MergeUnit(level, maxNumComponents, EXTRA_COMPONENTS, 0 /* base capacity not used */);
            }
        }
    }

    @Override
    protected MergeOperation createMergeOperaiton(int level, ISpeedProvider speedProvider, SubOperation subOperation) {
        if (level == 0) {
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, sizeRatioLevel0,
                    sizeRatio,
                    (int) Math.ceil(sizeRatioLevel0 * memoryComponentCapacity.max / diskComponentCapacity) + sizeRatio);
            op.numComponentsInCurrentLevel = sizeRatioLevel0;
            op.numComponentsInNextLevel = 0;
            return op;
        } else if (level == 1) {
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, maxNumStackedComponents,
                    sizeRatio * sizeRatio, sizeRatio * sizeRatio + 1);
            op.numComponentsInCurrentLevel = 1;
            op.numComponentsInNextLevel = 0;
            return op;
        } else {
            MergeOperation op = new MergeOperation(level, 0, subOperation, this, speedProvider, 1, sizeRatio * 2,
                    sizeRatio * 2 + 1);
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
        } else if (mergeUnit.level == 1) {
            if (mergeUnit.numComponents >= sizeRatioLevel0) {
                return true;
            }
            HybridMergeUnit hybridUnit = (HybridMergeUnit) mergeUnit;
            for (int i = 0; i < hybridUnit.usedNumStacks; i++) {
                if (hybridUnit.numStackedComponents[i] == maxNumStackedComponents) {
                    return true;
                }
            }
            return false;
        } else {
            return mergeUnit.numComponents >= mergeUnit.maxNumComponents;
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
            double resultRatio = 1.0;
            //double resultRatio = LSMFlowControlUtils.nextGaussian(mergeComponentRatios[0][0]);
            double resultCapacity = totalCapacity * resultRatio;
            mergeOp.reset(resultCapacity, pageEstimator.estiamtePages(resultCapacity));

            buildMergeOutputComponentsForLevel0(mergeOp, mergeUnits[1]);
            assert mergeOp.numComponentsInNextLevel == 0;

            if (DEBUG) {
                LOGGER.error("{}: Schedule merge level 0 components {} into {}. operations {}", time,
                        toString(mergeOp.componentsInCurrentLevel, mergeOp.numComponentsInCurrentLevel),
                        toString(mergeOp.outputComponents, mergeOp.numOutputComponents), numRunningOperations + 1);
            }
            return mergeOp;
        } else if (mergeUnit.level == 1) {
            while (isMergeable(mergeUnit)) {
                MergeUnit nextUnit = mergeUnits[mergeUnit.level + 1];
                boolean selected = selectHybridMergeComponents(mergeOp, (HybridMergeUnit) mergeUnit, nextUnit);
                if (!selected) {
                    if (DEBUG) {
                        LOGGER.error("{}: Cannot schedule merge for level 1", time);
                    }
                    nextUnit.setBlockedUnit(mergeUnit);
                    return null;
                }
                if (mergeOp.numComponentsInNextLevel == 0 && mergeOp.numComponentsInCurrentLevel == 1) {
                    // we can push
                    mergeOp.componentsInCurrentLevel[0].pin();
                    addComponent(nextUnit, mergeOp.componentsInCurrentLevel[0]);
                    assert sanityCheck(nextUnit.components, nextUnit.numComponents);
                    removeHybridComponents((HybridMergeUnit) mergeUnit, mergeOp.componentsInCurrentLevel,
                            mergeOp.numComponentsInCurrentLevel, mergeOp.index);
                    if (DEBUG) {
                        LOGGER.error("{}: Push a level {} component {} to level {}", time, mergeUnit.level,
                                toString(mergeOp.componentsInCurrentLevel, 1), mergeUnit.level + 1);
                    }
                    notifyBlockedUnit(mergeUnit);
                } else {

                    // schedule the merge now
                    double totalCapacity = 0;
                    for (int i = 0; i < mergeOp.numComponentsInCurrentLevel; i++) {
                        totalCapacity += mergeOp.componentsInCurrentLevel[i].records;
                        mergeOp.componentsInCurrentLevel[i].isMerging = true;
                    }
                    Component sumComponent = Component.get();
                    sumComponent.reset(mergeOp.componentsInCurrentLevel[0].min, mergeOp.componentsInCurrentLevel[0].max,
                            totalCapacity);
                    for (int i = 0; i < mergeOp.numComponentsInNextLevel; i++) {
                        mergeOp.componentsInNextLevel[i].isMerging = true;
                        totalCapacity += mergeOp.componentsInNextLevel[i].records;
                    }
                    double resultRatio = 1;
                    double resultCapacity = totalCapacity * resultRatio;
                    // build output components
                    mergeOp.reset(resultCapacity, pageEstimator.estiamtePages(resultCapacity));

                    if (mergeOp.numComponentsInNextLevel == 0) {
                        mergeOp.numOutputComponents =
                                buildMergeOutputComponentsForLevel0(mergeOp.componentsInCurrentLevel,
                                        mergeOp.numComponentsInCurrentLevel, totalCapacity, mergeOp.outputComponents);
                    } else {
                        mergeOp.numOutputComponents = buildMergeOutputComponents(new Component[] { sumComponent }, 1,
                                mergeOp.componentsInNextLevel, mergeOp.numComponentsInNextLevel,
                                mergeOp.outputComponents);
                    }
                    sumComponent.unpin();
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
            }
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
                    notifyBlockedUnit(mergeUnit);
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
                notifyBlockedUnit(mergeUnit);
                return null;
            }
        }
        return null;
    }

    private boolean selectHybridMergeComponents(MergeOperation mergeOp, HybridMergeUnit mergeUnit, MergeUnit nextUnit) {
        assert mergeUnit.usedNumStacks > 0;
        // TODO: we should definitely optimize it
        double currentPriority = Double.MAX_VALUE;
        int selectedNumStackedComponents = 0;
        int selectedNumComponentsInNextLevel = 0;
        int selectedBeginIndexCurrentLevel = 0;
        int selectedBeginIndexNextLevel = 0;
        for (int i = 0; i < mergeUnit.usedNumStacks; i++) {
            if (mergeUnit.numStackedComponents[i] == 0 || mergeUnit.stackedComponents[i][0].isMerging) {
                continue;
            }
            int beginIndex = getStartComponentIndex(nextUnit.components, nextUnit.numComponents,
                    mergeUnit.stackedComponents[i][0]);
            int endIndex = beginIndex;
            boolean isMerging = false;
            for (; endIndex < nextUnit.numComponents; endIndex++) {
                if (DoubleUtil.lessThanOrEqualTo(mergeUnit.stackedComponents[i][0].max,
                        nextUnit.components[endIndex].min)) {
                    break;
                }
                if (nextUnit.components[endIndex].isMerging) {
                    isMerging = true;
                    break;
                }
            }
            if (isMerging) {
                continue;
            }
            int numOverlappingComponents = endIndex - beginIndex;
            assert numOverlappingComponents >= 0;
            double newPriority = 0;
            if (mergeUnit.numStackedComponents[i] == maxNumStackedComponents) {
                newPriority = -1.0 / numOverlappingComponents;
            } else if (numOverlappingComponents == 0) {
                newPriority = 1 / (double) mergeUnit.numStackedComponents[i];
            } else {
                newPriority = numOverlappingComponents / (double) mergeUnit.numStackedComponents[i];
            }
            if (newPriority < currentPriority) {
                // In case of tie, we should choose the one with most/fewest components?
                selectedNumComponentsInNextLevel = numOverlappingComponents;
                selectedBeginIndexCurrentLevel = i;
                selectedBeginIndexNextLevel = beginIndex;
                currentPriority = newPriority;
            }
        }
        if (currentPriority == Double.MAX_VALUE) {
            return false;
        } else {
            mergeOp.index = selectedBeginIndexCurrentLevel;
            for (int i = 0; i < mergeUnit.numStackedComponents[selectedBeginIndexCurrentLevel]; i++) {
                mergeOp.componentsInCurrentLevel[i] = mergeUnit.stackedComponents[selectedBeginIndexCurrentLevel][i];
            }
            mergeOp.numComponentsInCurrentLevel = mergeUnit.numStackedComponents[selectedBeginIndexCurrentLevel];
            for (int j = selectedBeginIndexNextLevel; j < selectedBeginIndexNextLevel
                    + selectedNumComponentsInNextLevel; j++) {
                assert j - selectedBeginIndexNextLevel < mergeOp.componentsInNextLevel.length;
                mergeOp.componentsInNextLevel[j - selectedBeginIndexNextLevel] = nextUnit.components[j];
            }
            mergeOp.numComponentsInNextLevel = selectedNumComponentsInNextLevel;
            return true;
        }
    }

    private void finalizeMergeOperationInLevel0(MergeUnit mergeUnit) {
        HybridMergeUnit nextUnit = (HybridMergeUnit) mergeUnits[1];
        MergeOperation op = mergeUnit.getOperation();
        if (nextUnit.numComponents == 0) {
            // we simply put components to the next unit
            for (int i = 0; i < op.numOutputComponents; i++) {
                nextUnit.stackedComponents[i][0] = op.outputComponents[i];
                nextUnit.numStackedComponents[i] = 1;
                nextUnit.componentRanges[i] = op.outputComponents[i].max;
            }
            nextUnit.numComponents = op.numOutputComponents;
            nextUnit.usedNumStacks = op.numOutputComponents;
        } else {
            // we first check whether each component has room
            for (int i = 0; i < nextUnit.usedNumStacks; i++) {
                if (nextUnit.numStackedComponents[i] == nextUnit.maxNumStackedComponents) {
                    nextUnit.setBlockedUnit(mergeUnit);
                    // cannot proceed
                    return;
                }
            }
            assert nextUnit.usedNumStacks == op.numOutputComponents;
            for (int i = 0; i < nextUnit.usedNumStacks; i++) {
                nextUnit.stackedComponents[i][nextUnit.numStackedComponents[i]] = op.outputComponents[i];
                nextUnit.numStackedComponents[i]++;
            }
            nextUnit.numComponents += op.numOutputComponents;
        }

        // we have added components to the next level
        assert op.numComponentsInCurrentLevel == sizeRatioLevel0;
        for (int i = 0; i < op.numComponentsInCurrentLevel; i++) {
            mergeUnit.components[i].unpin();
        }
        for (int i = 0; i < mergeUnit.numComponents - op.numComponentsInCurrentLevel; i++) {
            mergeUnit.components[i] = mergeUnit.components[i + sizeRatioLevel0];
        }
        mergeUnit.numComponents -= op.numComponentsInCurrentLevel;

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
        if (mergeUnit.level == 1) {
            removeHybridComponents((HybridMergeUnit) mergeUnit, op.componentsInCurrentLevel,
                    op.numComponentsInCurrentLevel, op.index);
            assert sanityCheck((HybridMergeUnit) mergeUnit);
        } else {
            removeComponent(mergeUnit, op.componentsInCurrentLevel[0]);
            assert sanityCheck(mergeUnit.components, mergeUnit.numComponents);
        }
        if (op.numComponentsInNextLevel > 0) {
            replaceComponents(nextUnit, op.componentsInNextLevel, op.numComponentsInNextLevel, op.outputComponents,
                    op.numOutputComponents);
        } else {
            // add components
            int startIndex =
                    getStartComponentIndex(nextUnit.components, nextUnit.numComponents, op.outputComponents[0]);
            // add rooms
            for (int i = nextUnit.numComponents - 1; i >= startIndex; i--) {
                nextUnit.components[i + op.numOutputComponents] = nextUnit.components[i];
            }
            for (int i = startIndex; i < startIndex + op.numOutputComponents; i++) {
                nextUnit.components[i] = op.outputComponents[i - startIndex];
            }
            nextUnit.numComponents += op.numOutputComponents;
        }

        assert sanityCheck(nextUnit.components, nextUnit.numComponents);
        mergeUnit.operation = null;
        notifyBlockedUnit(mergeUnit);

        if (isMergeable(nextUnit)) {
            scheduleMergeOperation(nextUnit);
        }
        // we should schedule more merges
        if (isMergeable(mergeUnit)) {
            scheduleMergeOperation(mergeUnit);
        }
    }

    private void notifyBlockedUnit(MergeUnit mergeUnit) {
        if (mergeUnit.blockedUnit != null) {
            MergeUnit blockedUnit = (MergeUnit) mergeUnit.blockedUnit;
            if (mergeUnit.level == 1
                    && ((HybridMergeUnit) mergeUnit).getCurrentMaxStackedComponents() < maxNumStackedComponents) {
                mergeUnit.unsetBlockedUnit();
                finalizeMergeOperationInLevel0(blockedUnit);
            } else {
                // schedule a merge at blocked
                if (blockedUnit.operation != null) {
                    MergeOperation op = blockedUnit.getOperation();
                    if (blockedUnit.operation.isCompleted && mergeUnit.numComponents - op.numComponentsInNextLevel
                            + op.numOutputComponents < mergeUnit.maxNumComponents + mergeUnit.extraNumComponents) {
                        mergeUnit.unsetBlockedUnit();
                        finalizeMergeOperation(blockedUnit);
                    }
                } else {
                    mergeUnit.unsetBlockedUnit();
                    if (isMergeable(blockedUnit)) {
                        scheduleMergeOperation(blockedUnit);
                    }
                }
            }
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
            finalizeMergeOperationInLevel0(mergeUnit);
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

    protected void buildMergeOutputComponentsForLevel0(MergeOperation mergeOp, MergeUnit nextUnit) {
        if (nextUnit.numComponents == 0) {
            mergeOp.numOutputComponents = super.buildMergeOutputComponentsForLevel0(mergeOp.componentsInCurrentLevel,
                    mergeOp.numComponentsInCurrentLevel, mergeOp.totalCapacity, mergeOp.outputComponents);
        } else {
            mergeOp.numOutputComponents = 0;
            HybridMergeUnit hybridUnit = (HybridMergeUnit) nextUnit;
            assert hybridUnit.usedNumStacks > 0;
            double minRange = MIN_RANGE;
            for (int i = 0; i < hybridUnit.usedNumStacks; i++) {
                double maxRange = hybridUnit.componentRanges[i];
                assert maxRange > 0;
                double diskComponentCapacity = (maxRange - minRange) / (MAX_RANGE - MIN_RANGE) * mergeOp.totalCapacity;
                Component outputComponent = Component.get();
                outputComponent.reset(minRange, maxRange, diskComponentCapacity);
                mergeOp.outputComponents[mergeOp.numOutputComponents++] = outputComponent;
                minRange = maxRange;
            }
        }
    }

    private void removeHybridComponents(HybridMergeUnit mergeUnit, Component[] components, int numComponents,
            int index) {
        if (numComponents == 0) {
            return;
        }
        for (int i = 0; i < numComponents; i++) {
            assert mergeUnit.stackedComponents[index][i] == components[i];
            mergeUnit.stackedComponents[index][i].unpin();
        }
        for (int i = 0; i < mergeUnit.numStackedComponents[index] - numComponents; i++) {
            assert mergeUnit.stackedComponents[index][i + numComponents] != null;
            mergeUnit.stackedComponents[index][i] = mergeUnit.stackedComponents[index][i + numComponents];
        }
        mergeUnit.numStackedComponents[index] -= numComponents;
        mergeUnit.numComponents -= numComponents;
    }

    private boolean sanityCheck(HybridMergeUnit mergeUnit) {
        double lastMax = -Double.MAX_VALUE;
        for (int i = 0; i < mergeUnit.usedNumStacks; i++) {
            if (mergeUnit.numStackedComponents[i] > 0) {
                assert mergeUnit.stackedComponents[i][0].min >= lastMax;
                assert mergeUnit.stackedComponents[i][0].max > mergeUnit.stackedComponents[i][0].min;
                lastMax = mergeUnit.stackedComponents[i][0].max;
            }
        }
        return true;
    }

}
