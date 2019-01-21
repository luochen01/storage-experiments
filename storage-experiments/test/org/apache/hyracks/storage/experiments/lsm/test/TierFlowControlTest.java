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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.MergePolicyType;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.ILSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.RandomVariable;

public class TierFlowControlTest {

    private static final int sizeRatio = 3;

    @Test
    public void simulate() throws IOException {
        int toleratedComponents = 3;
        int numMemoryComponents = 4;
        int numLevels = 5;
        RandomVariable memoryComponentCapacity = RandomVariable.of(10000, 0, 10000, 10000);
        double baseTime = 0.1;
        RandomVariable[] flushProcessingTimes = prepareProcessingTimes(baseTime, numLevels);
        RandomVariable[] flushFinalizeTimes = prepareProcessingTimes(baseTime, numLevels);
        RandomVariable[][] mergeProcessingTimes = prepareMergeProcessingTimes(baseTime, numLevels);
        RandomVariable[][] mergeFinalizeTimes = prepareMergeProcessingTimes(baseTime, numLevels);
        RandomVariable[][] componentRatios = new RandomVariable[numLevels][1];
        Arrays.fill(componentRatios, new RandomVariable[] { RandomVariable.of(1, 0, 1, 1) });
        double[] initialUsedMemory = {};
        double initialCurrentUsedMemory = 0;
        double initialFlushCapacity = 0;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0;
        double[][] initialComponents = new double[numLevels][0];
        double[] initialMergedComponents = new double[numLevels];
        double[] initialMergeFinalizedPages = {};
        double[] initialMergeSubOperationElapsedTimes = {};
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 10000;
        double recordsPerPage = 100;
        ILSMFinalizingPagesEstimator estimator = new ILSMFinalizingPagesEstimator() {
            @Override
            public double estiamtePages(double records) {
                return 0;
            }
        };
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, numLevels,
                memoryComponentCapacity, numMemoryComponents, sizeRatio, 100000, flushProcessingTimes,
                flushFinalizeTimes, mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory,
                initialCurrentUsedMemory, initialFlushCapacity, initialFlushFinalizedPages,
                initialFlushSubOperationElapsedTime, initialComponents, initialMergedComponents,
                initialMergeFinalizedPages, initialMergeSubOperationElapsedTimes, componentRatios, estimator,
                MergePolicyType.TIER, recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);
        System.out.println(solver.solveMaxBlockingSpeed());
    }

    private RandomVariable[] prepareProcessingTimes(double baseTime, int levels) {
        RandomVariable[] variables = new RandomVariable[levels + 2];
        for (int i = 0; i < variables.length; i++) {
            variables[i] = RandomVariable.of(baseTime * i, 0, baseTime * i, baseTime * i);
        }
        return variables;
    }

    private RandomVariable[][] prepareMergeProcessingTimes(double baseTime, int levels) {
        RandomVariable[][] variables = new RandomVariable[levels][];
        for (int i = 0; i < levels; i++) {
            variables[i] = prepareProcessingTimes(baseTime, levels);
        }
        return variables;
    }

}
