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

import java.util.ArrayDeque;
import java.util.Queue;

class Component {
    private static Queue<Component> components = new ArrayDeque<>();

    public double min;
    public double max;
    private double range;
    public double records;

    public boolean isMerging;
    public boolean isFull;
    private int pinCount = 0;

    private static int requestedComponents = 0;
    private static int returnedComponents = 0;

    private Component() {
    }

    public static Component get() {
        requestedComponents++;
        Component component = null;
        if (components.isEmpty()) {
            component = new Component();
        } else {
            component = components.poll();
        }
        component.pin();
        return component;
    }

    public static void print() {
        System.out.println("requested: " + requestedComponents + " returned: " + returnedComponents + " diff: "
                + (requestedComponents - returnedComponents));
    }

    public void pin() {
        assert pinCount >= 0;
        pinCount++;
        // System.out.println("Pin " + this.hashCode() + " pin count " + pinCount);
    }

    public void unpin() {
        assert pinCount > 0;

        pinCount--;
        // System.out.println("Unpin " + this.hashCode() + " pin count " + pinCount);
        assert pinCount >= 0;
        if (pinCount == 0) {
            returnedComponents++;
            components.add(this);
        }
    }

    public void reset(double min, double max, double records) {
        this.min = min;
        this.max = max;
        this.records = records;
        this.isMerging = false;
        this.isFull = false;
        this.range = max - min;
        assert this.range > 0;
        if (records < 0.001) {
            System.out.println();
        }
    }

    public void initialize(double records) {
        this.records = records;
        this.isMerging = false;
        this.isFull = false;
    }

    @Override
    public String toString() {
        return "[" + min + "," + max + "]" + ":" + String.valueOf(records);
    }

    public double getRange() {
        return range;
    }

}

class SubOperation implements Comparable<SubOperation> {
    public double totalCapacity;
    public double totalTime;
    public double remainingTime;
    public boolean active;

    public final int subOperationType;
    private final IOperationScheduler scheduler;

    public SubOperation(int subOperationType, IOperationScheduler scheduler) {
        this.subOperationType = subOperationType;
        this.scheduler = scheduler;
    }

    public void reset() {
        this.active = false;
    }

    public void initialize(double totalCapacity, double totalTime) {
        assert !Double.isInfinite(totalTime);
        //assert totalCapacity > 0;
        this.totalCapacity = totalCapacity;
        this.totalTime = totalTime;
        this.remainingTime = totalTime;
        this.active = true;
        this.scheduler.addNewSubOperation(this);
    }

    public boolean isCompleted() {
        return remainingTime <= 0;
    }

    public double getRemainingTime() {
        return remainingTime;
    }

    @Override
    public int compareTo(SubOperation o) {
        return Double.compare(remainingTime, o.remainingTime);
    }

}

enum IoOperationPhase {
    Processing,
    Finalizing
}

class IoOperation {
    public IoOperationPhase phase;
    public double totalCapacity;
    public double currentCapacity;
    public double totalFinalizePages;
    public double currentFinalizePages;
    public final SubOperation subOperation;
    public final IOperationScheduler scheduler;
    public final ISpeedProvider speedProvider;

    public boolean isCompleted;

    public final Component[] outputComponents;
    public int numOutputComponents;

    public IoOperation(double totalCapacity, SubOperation subOp, IOperationScheduler scheduler,
            ISpeedProvider speedProvider, int maxNumOutputComponents) {
        this.totalCapacity = totalCapacity;
        this.currentCapacity = 0;
        this.subOperation = subOp;
        this.scheduler = scheduler;
        this.speedProvider = speedProvider;
        this.outputComponents = new Component[maxNumOutputComponents];
    }

    public boolean finishedProcessing() {
        return this.phase == IoOperationPhase.Finalizing && this.currentFinalizePages >= this.totalFinalizePages;
    }

    public double getRemainingCapacity() {
        return totalCapacity - currentCapacity;
    }

    public void setCompleted() {
        this.isCompleted = true;
    }

    public void reset(double totalCapacity, double finalizePages) {
        this.isCompleted = false;
        this.phase = IoOperationPhase.Processing;
        this.currentCapacity = 0;
        this.currentFinalizePages = 0;
        this.totalCapacity = totalCapacity;
        this.totalFinalizePages = finalizePages;
        this.subOperation.reset();
    }

    public void initialize(double processedCapacity, double finalizedPages) {
        this.currentCapacity = processedCapacity;
        this.currentFinalizePages = finalizedPages;

        if (this.currentFinalizePages > 0) {
            this.phase = IoOperationPhase.Finalizing;
        } else {
            this.phase = IoOperationPhase.Processing;
            if (this.currentCapacity >= this.totalCapacity) {
                // advance to finalize
                this.currentCapacity = this.totalCapacity;
                this.phase = IoOperationPhase.Finalizing;
            }
        }
    }

    public void initializeNewSubOperation() {
        assert subOperation.isCompleted() || !subOperation.active;
        assert phase != null;
        switch (phase) {
            case Processing:
                // create a new subOperation
                double subOperationCapacity = scheduler.getSubOperationProcessingRecords();
                double processingTime = speedProvider.getProcessingSpeed(scheduler.getNumRunningOperations());
                double remainingCapacity = totalCapacity - currentCapacity;
                if (remainingCapacity < subOperationCapacity) {
                    // should be rare
                    double remainingTime = processingTime * (remainingCapacity / subOperationCapacity);
                    subOperation.initialize(remainingCapacity, remainingTime);
                } else {
                    subOperation.initialize(subOperationCapacity, processingTime);
                }
                break;
            case Finalizing:
                double finalizeTime = speedProvider.getFinalizeSpeed(scheduler.getNumRunningOperations());
                double finalizePages = scheduler.getSubOperationPages();
                double remainingPages = totalFinalizePages - currentFinalizePages;
                if (remainingPages < finalizePages) {
                    double remainingTime = finalizeTime * (remainingPages / finalizePages);
                    subOperation.initialize(remainingPages, remainingTime);
                } else {
                    subOperation.initialize(finalizePages, finalizeTime);
                }
                break;
            default:
                throw new IllegalStateException("Illegal phase " + phase);
        }
    }

    public boolean completeSubOperation() {
        assert subOperation.isCompleted();
        switch (phase) {
            case Processing:
                assert subOperation.totalCapacity > 0;
                currentCapacity += subOperation.totalCapacity;
                if (currentCapacity >= totalCapacity) {
                    phase = IoOperationPhase.Finalizing;
                }
                return false;
            case Finalizing:
                currentFinalizePages += subOperation.totalCapacity;
                return currentFinalizePages >= totalFinalizePages;
            default:
                throw new IllegalStateException("Illegal phase " + phase);
        }
    }
}

class FlushOperation extends IoOperation {

    public FlushOperation(double totalCapacity, SubOperation subOp, IOperationScheduler scheduler,
            ISpeedProvider speedProvider) {
        super(totalCapacity, subOp, scheduler, speedProvider, 1);
    }
}

class MergeOperation extends IoOperation {
    public final int level;

    public final Component[] componentsInCurrentLevel;
    public int numComponentsInCurrentLevel;
    public int index;

    public final Component[] componentsInNextLevel;
    public int numComponentsInNextLevel;

    public MergeOperation(int level, double totalCapacity, SubOperation subOp, IOperationScheduler scheduler,
            ISpeedProvider speedProvider, int maxNumComponentsInCurrentLevel, int maxNumComponentsInNextLevel,
            int maxNumOutputComponents) {
        super(0, subOp, scheduler, speedProvider, maxNumOutputComponents);
        this.level = level;
        this.componentsInCurrentLevel = new Component[maxNumComponentsInCurrentLevel];
        this.componentsInNextLevel = new Component[maxNumComponentsInNextLevel];
    }

}
