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

public abstract class AbstractPartitionedLevelMergeScheduler extends AbstractOperationScheduler {
    private static final Logger LOGGER = LogManager.getLogger(AbstractPartitionedLevelMergeScheduler.class);
    public static boolean DEBUG = false;

    protected static final double MIN_RANGE = 0;

    protected static final double MAX_RANGE = 1000000;

    protected final RandomVariable[][] mergeComponentRatios;

    protected final int sizeRatioLevel0;

    protected final double[] lastMergeKeys;

    protected final int toleratedComponentsLevel0;

    // for now, we assume each disk component has fixed-size
    protected final double diskComponentCapacity;

    protected static final int EXTRA_COMPONENTS = 2;

    protected int globalLSN = 0;

    public AbstractPartitionedLevelMergeScheduler(int toleratedComponentsPerLevel, int numLevels,
            RandomVariable memoryComponentCapacity, int totalMemoryComponents, int sizeRatio, ISpeedProvider flushSpeed,
            ISpeedProvider[] mergeSpeeds, ILSMFinalizingPagesEstimator pageEstimator,
            double subOperationProcessingRecords, double subOperationPages, double baseLevelCapacity,
            RandomVariable[][] mergeComponentRatios, int sizeRatioLevel0, int toleratedComponentsLevel0,
            double diskComponentCapacity) {
        super(toleratedComponentsPerLevel, numLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio,
                flushSpeed, mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                baseLevelCapacity);
        this.mergeComponentRatios = mergeComponentRatios;
        assert toleratedComponentsPerLevel >= 1;
        this.diskComponentCapacity = diskComponentCapacity;
        this.sizeRatioLevel0 = sizeRatioLevel0;
        this.toleratedComponentsLevel0 = toleratedComponentsLevel0;
        this.lastMergeKeys = new double[10];
    }

    protected boolean selectMergeComponents(int level, MergeOperation mergeOp, Component[] components,
            int numComponents, MergeUnit nextUnit) {
        // TODO: we should definitely optimize it
        int componentIndex = getNextComponentIndex(components, numComponents, lastMergeKeys[level]);
        if (components[componentIndex].isMerging) {
            return false;
        }

        mergeOp.componentsInCurrentLevel[0] = components[componentIndex];

        int nextIndex =
                getStartComponentIndex(nextUnit.components, nextUnit.numComponents, components[componentIndex].min);
        if (nextIndex < 0 || nextIndex >= nextUnit.numComponents) {
            mergeOp.numComponentsInNextLevel = 0;
        } else {
            int num = 0;
            for (int j = nextIndex; j < nextUnit.numComponents; j++) {
                if (nextUnit.components[j].min <= components[componentIndex].max) {
                    if (nextUnit.components[j].isMerging) {
                        return false;
                    }
                    mergeOp.componentsInNextLevel[j - nextIndex] = nextUnit.components[j];
                    num++;
                } else {
                    break;
                }
            }
            mergeOp.numComponentsInNextLevel = num;
            lastMergeKeys[level] = components[componentIndex].max;
        }
        lastMergeKeys[level] = components[componentIndex].max;
        return true;
    }

    protected int buildMergeOutputComponentsForLevel0(Component[] components, int numComponents, double totalCapacity,
            Component[] outputComponents) {
        // if the next unit is empty, we just do a direct partitioning
        // for now, we simply assume all data are uniformed distributed across the key space
        int numOutputComponents = (int) Math.ceil(totalCapacity / diskComponentCapacity);
        assert numOutputComponents > 0;

        double subRange = diskComponentCapacity / totalCapacity * components[0].getRange();
        assert subRange <= components[0].getRange() || numOutputComponents == 1;
        double currentSubRange = components[0].min;
        int minLSN = getMinLSN(components, numComponents);
        for (int i = 0; i < numOutputComponents - 1; i++) {
            Component outputComponent = Component.get();
            double nextSubRange = currentSubRange + subRange;
            outputComponent.reset(currentSubRange, nextSubRange, diskComponentCapacity, minLSN);
            currentSubRange = nextSubRange;
            outputComponents[i] = outputComponent;
        }

        Component lastComponent = Component.get();
        lastComponent.reset(currentSubRange, components[numComponents - 1].max,
                totalCapacity - diskComponentCapacity * (numOutputComponents - 1), minLSN);
        outputComponents[numOutputComponents - 1] = lastComponent;
        return numOutputComponents;
    }

    protected int buildMergeOutputComponents(Component[] componentsInCurrentLevel, int numComponentsInCurrentLevel,
            Component[] componentsInNextLevel, int numComponentsInNextLevel, Component[] outputComponents) {
        // TODO: integrate merge component ratios here
        // we need to build up the new components here
        int currentLevelIndex = 0;
        int nextLevelIndex = 0;

        assert numComponentsInCurrentLevel > 0 && numComponentsInNextLevel > 0;
        double lastCompomentMin = Math.min(componentsInCurrentLevel[0].min, componentsInNextLevel[0].min);
        double lastLoopMin = lastCompomentMin;
        double records = 0;
        int numOutputComponents = 0;
        int lastMinLSN = Integer.MAX_VALUE;
        while (currentLevelIndex < numComponentsInCurrentLevel && nextLevelIndex < numComponentsInNextLevel) {
            Component currentLevelComponent = componentsInCurrentLevel[currentLevelIndex];
            Component nextLevelComponent = componentsInNextLevel[nextLevelIndex];
            // exhaust the next level component
            double currentCompomentMin = Math.max(lastLoopMin, currentLevelComponent.min);
            double nextComponentMin = Math.max(lastLoopMin, nextLevelComponent.min);

            double sum = (diskComponentCapacity - records)
                    + currentLevelComponent.records * (currentCompomentMin) / currentLevelComponent.getRange()
                    + nextLevelComponent.records * (nextComponentMin) / nextLevelComponent.getRange();
            double ratio = (currentLevelComponent.records / currentLevelComponent.getRange()
                    + nextLevelComponent.records / nextLevelComponent.getRange());
            double tmpMax = sum / ratio;
            if (DoubleUtil.lessThanOrEqualTo(tmpMax, Math.min(currentLevelComponent.max, nextLevelComponent.max))) {
                // the component has been produced
                Component newComponent = Component.get();
                newComponent.reset(lastCompomentMin, tmpMax, diskComponentCapacity,
                        Math.min(lastMinLSN, Math.min(currentLevelComponent.LSN, nextLevelComponent.LSN)));
                lastMinLSN = Integer.MAX_VALUE;
                lastCompomentMin = tmpMax;
                lastLoopMin = lastCompomentMin;
                records = 0;
                outputComponents[numOutputComponents++] = newComponent;
            } else {
                double actualMax = Math.min(tmpMax, Math.min(currentLevelComponent.max, nextLevelComponent.max));
                double consumedRecords = currentLevelComponent.records * (actualMax - currentCompomentMin)
                        / currentLevelComponent.getRange()
                        + nextLevelComponent.records * (actualMax - nextComponentMin) / nextLevelComponent.getRange();
                records = records + consumedRecords;
                assert records < diskComponentCapacity;
                lastLoopMin = actualMax;
                if (tmpMax > currentLevelComponent.max) {
                    lastMinLSN = Math.min(lastMinLSN, currentLevelComponent.LSN);
                    currentLevelIndex++;
                } else {
                    lastMinLSN = Math.min(lastMinLSN, nextLevelComponent.LSN);
                    nextLevelIndex++;
                }
            }
        }

        while (currentLevelIndex < numComponentsInCurrentLevel) {
            Component component = componentsInCurrentLevel[currentLevelIndex];
            double componentMin = Math.max(lastLoopMin, component.min);
            double tmpMax = (diskComponentCapacity - records) * component.getRange() / component.records + componentMin;
            if (DoubleUtil.greaterThan(tmpMax, component.max)) {
                double actualMax = component.max;
                double consumedRecords = component.records * (actualMax - componentMin) / component.getRange();
                records = records + consumedRecords;
                assert records < diskComponentCapacity;
                lastLoopMin = actualMax;
                lastMinLSN = Math.min(lastMinLSN, component.LSN);
                currentLevelIndex++;
            } else {
                Component newComponent = Component.get();
                newComponent.reset(lastCompomentMin, tmpMax, diskComponentCapacity,
                        Math.min(lastMinLSN, component.LSN));
                lastMinLSN = Integer.MAX_VALUE;
                outputComponents[numOutputComponents++] = newComponent;
                lastCompomentMin = tmpMax;
                lastLoopMin = lastCompomentMin;
                records = 0;
            }
        }

        while (nextLevelIndex < numComponentsInNextLevel) {
            Component component = componentsInNextLevel[nextLevelIndex];
            double componentMin = Math.max(lastLoopMin, component.min);
            double tmpMax = (diskComponentCapacity - records) * component.getRange() / component.records + componentMin;
            if (DoubleUtil.greaterThan(tmpMax, component.max)) {
                double actualMax = component.max;
                double consumedRecords = component.records * (actualMax - componentMin) / component.getRange();
                records = records + consumedRecords;
                assert records < diskComponentCapacity;
                lastLoopMin = actualMax;
                nextLevelIndex++;
                lastMinLSN = Math.min(lastMinLSN, component.LSN);
            } else {
                Component newComponent = Component.get();
                newComponent.reset(lastCompomentMin, tmpMax, diskComponentCapacity,
                        Math.min(lastMinLSN, component.LSN));
                lastMinLSN = Integer.MAX_VALUE;
                outputComponents[numOutputComponents++] = newComponent;
                lastCompomentMin = tmpMax;
                lastLoopMin = lastCompomentMin;
                records = 0;
            }
        }

        if (DoubleUtil.greaterThan(records, 0)) {
            Component newComponent = Component.get();
            newComponent.reset(lastCompomentMin,
                    Math.max(componentsInCurrentLevel[numComponentsInCurrentLevel - 1].max,
                            componentsInNextLevel[numComponentsInNextLevel - 1].max),
                    records, Math.min(componentsInCurrentLevel[numComponentsInCurrentLevel - 1].LSN,
                            componentsInNextLevel[numComponentsInNextLevel - 1].LSN));
            outputComponents[numOutputComponents++] = newComponent;
        }
        assert numOutputComponents > 0;
        return numOutputComponents;
    }

    protected int getStartComponentIndex(Component[] components, int numComponents, double key) {
        int startIndex = binarySearch(components, numComponents, key);
        if (startIndex < 0) {
            startIndex = -startIndex - 1;
        }
        if (startIndex < numComponents && components[startIndex] != null
                && DoubleUtil.equals(components[startIndex].max, key)) {
            startIndex++;
        }
        return startIndex;
    }

    protected int getNextComponentIndex(Component[] components, int numComponents, double key) {
        int index = getStartComponentIndex(components, numComponents, key);
        if (index < numComponents && components[index].max > key && components[index].min < key) {
            index++;
        }
        if (index >= numComponents) {
            return 0;
        } else {
            return index;
        }
    }

    protected int binarySearch(Component[] components, int numComponents, double key) {
        int low = 0;
        int high = numComponents - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Component midVal = components[mid];
            if (midVal.max <= key)
                low = mid + 1;
            else if (midVal.min > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1); // key not found.
    }

    protected void addComponent(MergeUnit unit, Component newComponent) {
        unit.numComponents = this.addComponent(unit.components, unit.numComponents, newComponent);
    }

    protected int addComponent(Component[] components, int numComponents, Component newComponent) {
        int startIndex = getStartComponentIndex(components, numComponents, newComponent.min);
        for (int i = numComponents - 1; i >= startIndex; i--) {
            components[i + 1] = components[i];
        }
        components[startIndex] = newComponent;
        return numComponents + 1;
    }

    protected void removeComponent(MergeUnit unit, Component component) {
        unit.numComponents = removeComponent(unit.components, unit.numComponents, component);
    }

    protected int removeComponent(Component[] components, int numComponents, Component component) {
        int index = getStartComponentIndex(components, numComponents, component.min);
        assert components[index] == component;
        components[index].unpin();
        for (int i = index; i < numComponents - 1; i++) {
            components[i] = components[i + 1];
        }
        components[numComponents - 1] = null;
        return numComponents - 1;
    }

    protected void replaceComponents(MergeUnit unit, Component[] sourceComponents, int numSourceComponents,
            Component[] newComponents, int numNewComponents) {
        int sourceComponentIndex = getStartComponentIndex(unit.components, unit.numComponents, sourceComponents[0].min);
        assert unit.components[sourceComponentIndex] == sourceComponents[0];
        if (numSourceComponents == numNewComponents) {
            // easy case
            for (int i = 0; i < numSourceComponents; i++) {
                assert unit.components[i + sourceComponentIndex] == sourceComponents[i];
                unit.components[i + sourceComponentIndex].unpin();
                unit.components[i + sourceComponentIndex] = newComponents[i];
            }
        } else if (numSourceComponents < numNewComponents) {
            // we have more new components to add
            int numAdditionalComponents = numNewComponents - numSourceComponents;
            for (int i = 0; i < numSourceComponents; i++) {
                assert unit.components[i + sourceComponentIndex] == sourceComponents[i];
                unit.components[i + sourceComponentIndex].unpin();
                unit.components[i + sourceComponentIndex] = newComponents[i];
            }
            // make some rooms for new components
            for (int i = unit.numComponents - 1; i >= sourceComponentIndex + numSourceComponents - 1; i--) {
                assert i + numAdditionalComponents < unit.components.length;
                unit.components[i + numAdditionalComponents] = unit.components[i];
            }
            for (int i = numSourceComponents; i < numNewComponents; i++) {
                unit.components[i + sourceComponentIndex] = newComponents[i];
            }
            unit.numComponents += numAdditionalComponents;
        } else {
            // we need to delete some components
            int numDiffComponents = numSourceComponents - numNewComponents;
            for (int i = 0; i < numNewComponents; i++) {
                assert unit.components[i + sourceComponentIndex] == sourceComponents[i];
                unit.components[i + sourceComponentIndex].unpin();
                unit.components[i + sourceComponentIndex] = newComponents[i];
            }
            for (int i = numNewComponents; i < numSourceComponents; i++) {
                assert unit.components[i + sourceComponentIndex] == sourceComponents[i];
                unit.components[i + sourceComponentIndex].unpin();
            }
            for (int i = sourceComponentIndex + numNewComponents; i < unit.numComponents - numDiffComponents; i++) {
                unit.components[i] = unit.components[i + numDiffComponents];
            }
            unit.numComponents -= numDiffComponents;
        }

    }

    protected boolean sanityCheck(Component[] components, int numComponents) {
        for (int i = 0; i < numComponents - 1; i++) {
            Component c1 = components[i];
            Component c2 = components[i + 1];
            assert c1.min < c1.max;
            assert DoubleUtil.lessThanOrEqualTo(c1.max, c2.min);
        }
        return true;
    }

    @Override
    protected FlushOperation initializeFlushOperation(double totalCapacity) {
        flushOperation.reset(totalCapacity, pageEstimator.estiamtePages(totalCapacity));
        Component outputComponent = Component.get();
        outputComponent.reset(MIN_RANGE, MAX_RANGE, totalCapacity, ++globalLSN);
        flushOperation.outputComponents[0] = outputComponent;
        flushOperation.numOutputComponents = 1;
        return flushOperation;
    }

    protected int getMinLSN(Component[] components, int numComponents) {
        int minLSN = Integer.MAX_VALUE;
        for (int i = 0; i < numComponents; i++) {
            minLSN = Math.min(minLSN, components[i].LSN);
        }
        return minLSN;
    }

    public String toString(Component[] components, int numComponents) {
        if (numComponents == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numComponents - 1; i++) {
            sb.append("[" + (int) components[i].min + "," + (int) components[i].max + "]:" + (int) components[i].records
                    + ":" + (int) components[i].LSN);
            sb.append(", ");
        }
        sb.append("[" + (int) components[numComponents - 1].min + "," + (int) components[numComponents - 1].max + "]:"
                + (int) components[numComponents - 1].records + ":" + (int) components[numComponents - 1].LSN);
        return sb.toString();
    }

}
