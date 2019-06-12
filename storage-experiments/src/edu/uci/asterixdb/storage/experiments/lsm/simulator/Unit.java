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

import java.util.Arrays;

class Unit {
    public IoOperation operation;

    public IoOperation getOperation() {
        return operation;
    }

    protected void initialize() {
        this.operation = null;
    }
}

class FlushUnit extends Unit {
    public final double[] maxCapacities;
    public final RandomVariable memoryComponentCapacity;
    private RandomVariable memoryComponentFillUpTime;

    public int numUsedCurrentComponents;
    public int currentComponentIndex;
    public int currentFlushIndex;
    public double currentCapacity;

    public final SubOperation ingestOp;
    public double ingestSpeed;

    public FlushUnit(int maxNumMemoryComponents, RandomVariable memoryComponentCapacity, SubOperation ingestOp) {
        this.memoryComponentCapacity = memoryComponentCapacity;
        maxCapacities = new double[maxNumMemoryComponents];
        this.ingestOp = ingestOp;
    }

    @Override
    public FlushOperation getOperation() {
        return (FlushOperation) operation;
    }

    public boolean isFull() {
        return numUsedCurrentComponents == maxCapacities.length;
    }

    public void setMemoryComponentFull() {
        this.numUsedCurrentComponents++;
        this.currentComponentIndex = (this.currentComponentIndex + 1) % maxCapacities.length;
        this.currentCapacity = 0;
        if (this.numUsedCurrentComponents < maxCapacities.length) {
            activeNextMemoryComponent();
        }
    }

    public void activeNextMemoryComponent() {
        // active the new component
        double totalTime = DoubleUtil.nextGaussian(memoryComponentFillUpTime);
        this.maxCapacities[currentComponentIndex] = totalTime * ingestSpeed;
        ingestOp.initialize(maxCapacities[currentComponentIndex], totalTime);
    }

    private void activeNextMemoryComponentNoRandom() {
        // active the new component
        double totalTime = memoryComponentFillUpTime.mean;
        this.maxCapacities[currentComponentIndex] = totalTime * ingestSpeed;
        ingestOp.initialize(maxCapacities[currentComponentIndex], totalTime);
    }

    public boolean ingest(double duration) {
        if (isFull()) {
            return true;
        }
        ingestOp.remainingTime -= duration;
        if (ingestOp.isCompleted()) {
            setMemoryComponentFull();
        }
        return false;
    }

    protected void initialize(double[] initialCapacities, double currentCapacity, double ingestSpeed) {
        super.initialize();
        this.memoryComponentFillUpTime = this.memoryComponentCapacity.multiply(1 / ingestSpeed);
        for (int i = 0; i < initialCapacities.length; i++) {
            maxCapacities[i] = initialCapacities[i];
        }
        for (int i = initialCapacities.length; i < maxCapacities.length; i++) {
            maxCapacities[i] = 0;
        }
        this.ingestSpeed = ingestSpeed;
        numUsedCurrentComponents = initialCapacities.length;

        this.currentFlushIndex = 0;
        this.currentComponentIndex = initialCapacities.length % maxCapacities.length;
        if (numUsedCurrentComponents < maxCapacities.length) {
            // active the new memory component
            activeNextMemoryComponentNoRandom();
            ingestOp.remainingTime -= currentCapacity / ingestSpeed;
            if (ingestOp.isCompleted()) {
                // full
                setMemoryComponentFull();
            }
        } else {
            assert DoubleUtil.equals(currentCapacity, 0);
            this.ingestOp.reset();
        }
    }
}

class MergeUnit extends Unit {
    public final int level;
    public final Component[] components;
    public final double baseCapacity;
    public Unit blockedUnit;
    public final int maxNumComponents;
    public final int extraNumComponents;
    public int numComponents = 0;

    public MergeUnit(int level, int maxNumComponents, int extraNumComponents, double baseCapacity) {
        this.level = level;
        this.maxNumComponents = maxNumComponents;
        this.components = new Component[maxNumComponents + extraNumComponents];
        this.baseCapacity = baseCapacity;
        this.extraNumComponents = extraNumComponents;
    }

    @Override
    public MergeOperation getOperation() {
        return (MergeOperation) operation;
    }

    public boolean isFull() {
        return numComponents >= maxNumComponents;
    }

    public void setBlockedUnit(Unit unit) {
        this.blockedUnit = unit;
    }

    public void unsetBlockedUnit() {
        assert this.blockedUnit != null;
        this.blockedUnit = null;
    }

    public void initialize(Component[] components) {
        super.initialize();
        this.blockedUnit = null;
        if (components != null) {
            this.numComponents = components.length;
            for (int i = 0; i < components.length; i++) {
                this.components[i] = components[i];
            }
            for (int i = components.length; i < this.components.length; i++) {
                this.components[i] = null;
            }
        } else {
            Arrays.fill(components, null);
            this.numComponents = 0;
        }
    }

    public void addComponent(Component component) {
        this.components[numComponents] = component;
        numComponents++;
    }

    public void removeComponent(int index) {
        assert index >= 0 && index < numComponents;
        this.components[index].unpin();
        for (int i = index; i < numComponents - 1; i++) {
            this.components[i] = this.components[i + 1];
        }
        this.components[numComponents - 1] = null;
        numComponents--;
    }

    public Component lastComponent() {
        assert numComponents > 0;
        return components[numComponents - 1];
    }

    @Override
    public String toString() {
        return "level " + level;
    }
}

class DualMergeUnit extends MergeUnit {

    public final Component[] dualComponents;
    public int numDualComponents;

    public DualMergeUnit(int level, int maxNumComponents, int extraNumComponents, double baseCapacity) {
        super(level, maxNumComponents, extraNumComponents, baseCapacity);
        this.dualComponents = new Component[maxNumComponents + extraNumComponents];
    }

    @Override
    public void initialize(Component[] components) {
        super.initialize(components);
        Arrays.fill(dualComponents, null);
        this.numDualComponents = 0;
    }

    public void initialize(Component[] components, Component[] dualComponents) {
        this.initialize(components);
        if (dualComponents != null) {
            this.numDualComponents = dualComponents.length;
            for (int i = 0; i < dualComponents.length; i++) {
                this.dualComponents[i] = dualComponents[i];
            }
            for (int i = dualComponents.length; i < this.dualComponents.length; i++) {
                this.dualComponents[i] = null;
            }
        }
    }

    public boolean readyToSwitch() {
        return numDualComponents == 0 && numComponents > maxNumComponents;
    }

    public void switchComponents() {
        // push components to dual components
        assert numDualComponents == 0;
        assert numComponents > 0;

        for (int i = 0; i < numComponents; i++) {
            dualComponents[i] = components[i];
            components[i] = null;
        }
        numDualComponents = numComponents;
        numComponents = 0;
    }

}