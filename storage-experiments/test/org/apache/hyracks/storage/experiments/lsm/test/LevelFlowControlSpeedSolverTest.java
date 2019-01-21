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

import org.junit.Assert;
import org.junit.Test;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.MergePolicyType;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.ILSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.LSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.RandomVariable;

public class LevelFlowControlSpeedSolverTest {
    private final int sizeRatio = 10;

    @Test
    public void test1() {
        int toleratedComponents = 1;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(77967.080000, 2993.949000, 71630.000000, 83578.000000);
        double maxIoSpeed = 89850.264603;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.336000, 0.029000, 0.297000, 0.417000),
                RandomVariable.of(0.449000, 0.089000, 0.342000, 0.687000),
                RandomVariable.of(0.556000, 0.200000, 0.317000, 1.042000),
                RandomVariable.of(0.741000, 0.200000, 0.422000, 1.389000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.826000, 0.161000, 2.670000, 3.098000),
                RandomVariable.of(8.222000, 3.905000, 4.011000, 16.812000),
                RandomVariable.of(6.069000, 2.588000, 3.742000, 12.280000),
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
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = {};
        double initialCurrentUsedMemory = 29638.000000;
        double initialFlushCapacity = 0.000000;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { { 75458.0 }, { 675458.0 }, { 475458 } };
        double[] initialMergedComponents = { 0.0, 1480192.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes = { 0.0, 0.436851911, 0.0 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.619475;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.619475, 7271.996450, 0.010000, 131072);
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, 3, memoryComponentCapacity,
                numMemoryComponents, sizeRatio, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes,
                mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory,
                initialFlushCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponents, initialMergedComponents, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes, componentRatios, estimator, MergePolicyType.LEVEL,
                recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);

        long speed = solver.solveMaxSpeedProbSampling();
        Assert.assertTrue(speed <= 4200 && speed >= 4100);
    }

    @Test
    public void test2() {
        int toleratedComponents = 1;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(78072.640000, 2996.723000, 71630.000000, 83578.000000);
        double maxIoSpeed = 102434.340164;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.294000, 0.047000, 0.257000, 0.535000),
                RandomVariable.of(0.382000, 0.081000, 0.286000, 0.683000),
                RandomVariable.of(0.528000, 0.182000, 0.277000, 0.977000),
                RandomVariable.of(0.704000, 0.182000, 0.370000, 1.303000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.913000, 0.152000, 2.673000, 3.207000),
                RandomVariable.of(5.000000, 2.667000, 2.944000, 9.877000),
                RandomVariable.of(7.246000, 4.693000, 3.209000, 18.414000),
                RandomVariable.of(9.662000, 4.693000, 4.278000, 24.552000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.340000, 0.043000, 0.299000, 0.550000),
                        RandomVariable.of(0.775000, 0.201000, 0.367000, 1.406000),
                        RandomVariable.of(1.217000, 0.450000, 0.401000, 2.484000),
                        RandomVariable.of(1.623000, 0.450000, 0.535000, 3.312000) },
                { null, RandomVariable.of(0.425000, 0.029000, 0.374000, 0.502000),
                        RandomVariable.of(0.816000, 0.175000, 0.533000, 1.242000),
                        RandomVariable.of(1.201000, 0.333000, 0.767000, 2.139000),
                        RandomVariable.of(1.602000, 0.333000, 1.023000, 2.853000) },
                { null, RandomVariable.of(0.457000, 0.026000, 0.408000, 0.517000),
                        RandomVariable.of(0.862000, 0.199000, 0.467000, 1.276000),
                        RandomVariable.of(1.453000, 0.439000, 0.950000, 2.385000),
                        RandomVariable.of(1.937000, 0.439000, 1.267000, 3.180000) } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(1.539000, 0.426000, 1.137000, 2.352000),
                        RandomVariable.of(2.925000, 1.003000, 1.738000, 4.541000),
                        RandomVariable.of(5.498000, 2.995000, 2.780000, 10.541000),
                        RandomVariable.of(7.331000, 2.995000, 3.707000, 14.055000) },
                { null, RandomVariable.of(0.694000, 0.284000, 0.301000, 1.191000),
                        RandomVariable.of(0.827000, 0.321000, 0.492000, 1.383000),
                        RandomVariable.of(1.240000, 0.321000, 0.737000, 2.074000),
                        RandomVariable.of(1.653000, 0.321000, 0.983000, 2.765000) },
                { RandomVariable.of(0.951000, 1.191000, 0.109000, 1.793000),
                        RandomVariable.of(0.951000, 1.191000, 0.109000, 1.793000),
                        RandomVariable.of(0.951000, 1.191000, 0.109000, 1.793000),
                        RandomVariable.of(0.951000, 1.191000, 0.109000, 1.793000),
                        RandomVariable.of(0.951000, 1.191000, 0.109000, 1.793000) } };
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
                        RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = {};
        double initialCurrentUsedMemory = 73254.000000;
        double initialFlushCapacity = 0.000000;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { {}, { 373254.0 }, { 2373254.0 } };
        double[] initialMergedComponents = { 0.0, 845824.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes = { 0.0, 0.016260093, 0.0 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.610989;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.610989, 7272.009977, 0.010000, 131072);
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, 3, memoryComponentCapacity,
                numMemoryComponents, sizeRatio, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes,
                mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory,
                initialFlushCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponents, initialMergedComponents, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes, componentRatios, estimator, MergePolicyType.LEVEL,
                recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);

        long speed = solver.solveMaxSpeedProbSampling();
        Assert.assertTrue(speed >= 4700 && speed <= 4800);
    }

}
