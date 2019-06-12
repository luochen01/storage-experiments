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

package org.apache.hyracks.storage.experiments.lsm.test;

import org.junit.Before;
import org.junit.Test;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.BlockingPartitionedLevelMergeScheduler;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.MergePolicyType;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.PartitionPolicy;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.ILSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.LSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.RandomVariable;

public class PartitionedLevelFlowControlSpeedSolverTest {
    private final int sizeRatio = 10;

    private final int sizeRatioLevel0 = 4;

    private final int toleratedComponentsLevel0 = sizeRatioLevel0;

    private final int baseLevelComponents = 4;

    @Before
    public void before() {
        FlowControlSpeedSolver.MAX_TIME = 3600 * 24;
    }

    private void printHeader() {
        System.out.println(
                "L1\tL0 current level costs\tL0 next level costs\tL0 merges\tL1 current level costs\tL1 next level costs\tL1 merges\tthroughput\tnon-blocking speed");
    }

    @Test
    public void testBlocking() {
        System.out.println("Blocking partition policy");
        PartitionPolicy policy = PartitionPolicy.Blocking;
        BlockingPartitionedLevelMergeScheduler.DEBUG = false;
        FlowControlSpeedSolver solver =
                createFlowControlSpeedSolver(policy, sizeRatioLevel0, toleratedComponentsLevel0, baseLevelComponents);
        solver.simulate(3000, true);
    }

    private FlowControlSpeedSolver createFlowControlSpeedSolver(PartitionPolicy partitionPolicy, int sizeRatioLevel0,
            int toleratedComponentsLevel0, double baseLevelComponents) {
        int toleratedComponents = 1;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(77967.080000, 2993.949000, 71630.000000, 83578.000000);
        double maxIoSpeed = 89850.264603;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.336000, 0.029000, 0.297000, 0.417000),
                RandomVariable.of(0.449000, 0.089000, 0.342000, 0.687000),
                RandomVariable.of(0.556000, 0.200000, 0.317000, 1.042000),
                RandomVariable.of(0.741000, 0.200000, 0.422000, 1.389000),
                RandomVariable.of(0.741000, 0.200000, 0.422000, 1.389000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.826000, 0.161000, 2.670000, 3.098000),
                RandomVariable.of(8.222000, 3.905000, 4.011000, 16.812000),
                RandomVariable.of(6.069000, 2.588000, 3.742000, 12.280000),
                RandomVariable.of(8.092000, 2.588000, 4.989000, 16.374000),
                RandomVariable.of(8.092000, 2.588000, 4.989000, 16.374000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.393000, 0.028000, 0.358000, 0.467000),
                        RandomVariable.of(0.838000, 0.224000, 0.508000, 1.492000),
                        RandomVariable.of(1.156000, 0.442000, 0.432000, 2.202000),
                        RandomVariable.of(1.541000, 0.442000, 0.576000, 2.936000) },
                { null, RandomVariable.of(0.482000, 0.032000, 0.425000, 0.600000),
                        RandomVariable.of(0.921000, 0.226000, 0.550000, 1.717000),
                        RandomVariable.of(1.386000, 0.500000, 0.809000, 4.219000),
                        RandomVariable.of(1.848000, 0.500000, 1.078000, 5.625000) },
                { null, RandomVariable.of(0.496000, 0.030000, 0.450000, 0.583000),
                        RandomVariable.of(1.322000, 0.402000, 0.559000, 2.659000),
                        RandomVariable.of(1.554000, 0.341000, 1.026000, 2.034000),
                        RandomVariable.of(2.072000, 0.341000, 1.367000, 2.713000) },
                { null, RandomVariable.of(0.496000, 0.030000, 0.450000, 0.583000),
                        RandomVariable.of(1.322000, 0.402000, 0.559000, 2.659000),
                        RandomVariable.of(1.554000, 0.341000, 1.026000, 2.034000),
                        RandomVariable.of(2.072000, 0.341000, 1.367000, 2.713000) } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(1.407000, 0.271000, 1.069000, 1.961000),
                        RandomVariable.of(2.889000, 1.437000, 1.285000, 5.823000),
                        RandomVariable.of(7.422000, 7.837000, 2.008000, 16.409000),
                        RandomVariable.of(9.896000, 7.837000, 2.677000, 21.879000) },
                { null, RandomVariable.of(0.464000, 0.103000, 0.356000, 0.594000),
                        RandomVariable.of(0.940000, 0.361000, 0.397000, 1.486000),
                        RandomVariable.of(1.411000, 0.361000, 0.595000, 2.228000),
                        RandomVariable.of(1.881000, 0.361000, 0.793000, 2.971000) },
                { RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000) },
                { RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000),
                        RandomVariable.of(1.371000, 1.762000, 0.125000, 2.617000) } };
        RandomVariable[][] componentRatios = {
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000),
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = {};
        double initialCurrentUsedMemory = 0.0;
        double initialFlushCapacity = 0.000000;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { {}, {}, {}, {} };
        double[] initialMergedComponents = { 0.0, 1480192.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes = { 0.0, 0.436851911, 0.0 };
        double subOperationPages = 128.000000;
        double recordsPerPage = 235.619475;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.619475, 7271.996450, 0.010000, 131072);
        int numLevels = 4;
        double diskComponentCapacity = 80000;
        FlowControlSpeedSolver solver =
                new FlowControlSpeedSolver(toleratedComponents, numLevels, memoryComponentCapacity, numMemoryComponents,
                        sizeRatio, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes, mergeProcessingTimes,
                        mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory, initialFlushCapacity,
                        initialFlushFinalizedPages, initialFlushSubOperationElapsedTime, initialComponents,
                        initialMergedComponents, initialMergeFinalizedPages, initialMergeSubOperationElapsedTimes,
                        componentRatios, estimator, MergePolicyType.LEVEL, recordsPerPage * subOperationPages,
                        subOperationPages, baseLevelComponents * diskComponentCapacity, true, sizeRatioLevel0,
                        toleratedComponentsLevel0, diskComponentCapacity, partitionPolicy);
        return solver;
    }
}