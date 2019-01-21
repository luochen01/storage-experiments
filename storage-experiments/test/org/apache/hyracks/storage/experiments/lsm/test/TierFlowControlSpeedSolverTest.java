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

import org.junit.Assert;
import org.junit.Test;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.MergePolicyType;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.ILSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.LSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.RandomVariable;

public class TierFlowControlSpeedSolverTest {

    private static final int sizeRatio = 3;

    @Test
    public void testInitialize() throws IOException {
        int toleratedComponents = 3;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(76252.000000, 3477.690000, 68962.000000, 82360.000000);
        double maxIoSpeed = 96390.341546;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.313000, 0.094000, 0.267000, 0.767000),
                RandomVariable.of(0.444000, 0.071000, 0.332000, 0.623000),
                RandomVariable.of(0.545000, 0.116000, 0.350000, 0.661000),
                RandomVariable.of(0.726000, 0.116000, 0.467000, 0.882000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.667000, 0.125000, 2.402000, 2.933000),
                RandomVariable.of(6.392000, 2.993000, 2.670000, 9.873000),
                RandomVariable.of(10.303000, 6.023000, 3.432000, 14.671000),
                RandomVariable.of(13.737000, 6.023000, 4.576000, 19.562000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.354000, 0.097000, 0.300000, 0.840000),
                        RandomVariable.of(0.456000, 0.067000, 0.333000, 0.616000),
                        RandomVariable.of(0.601000, 0.063000, 0.523000, 0.714000),
                        RandomVariable.of(0.802000, 0.063000, 0.697000, 0.952000) },
                { null, RandomVariable.of(0.335000, 0.020000, 0.308000, 0.373000),
                        RandomVariable.of(0.435000, 0.080000, 0.308000, 0.619000),
                        RandomVariable.of(0.572000, 0.070000, 0.417000, 0.698000),
                        RandomVariable.of(0.763000, 0.070000, 0.556000, 0.930000) },
                { null, null, null, null, null } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(2.015000, 0.144000, 1.780000, 2.136000),
                        RandomVariable.of(2.313000, 0.000000, 2.313000, 2.313000),
                        RandomVariable.of(8.532000, 0.499000, 8.179000, 8.885000),
                        RandomVariable.of(11.376000, 0.499000, 10.905000, 11.846000) },
                { RandomVariable.of(3.249000, 0.207000, 3.103000, 3.396000),
                        RandomVariable.of(3.249000, 0.207000, 3.103000, 3.396000),
                        RandomVariable.of(3.249000, 0.207000, 3.103000, 3.396000),
                        RandomVariable.of(3.249000, 0.207000, 3.103000, 3.396000),
                        RandomVariable.of(3.249000, 0.207000, 3.103000, 3.396000) },
                { null, null, null, null, null } };
        RandomVariable[][] componentRatios = { { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = { 76792.0 };
        double initialCurrentUsedMemory = 49425.000000;
        double initialFlushCapacity = 76792.000000;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0.133901;
        double[][] initialComponents =
                { { 73022.0, 69716.0 }, { 230956.0, 232870.0, 241976.0 }, { 667406.0, 695362.0 } };
        double[] initialMergedComponents = { 0.0, 241664.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes = { 0.0, 0.37016933599999996, 0.0 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.645036;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.645036, 7271.770162, 0.010000, 131072);
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, 3, memoryComponentCapacity,
                numMemoryComponents, sizeRatio, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes,
                mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory,
                initialFlushCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponents, initialMergedComponents, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes, componentRatios, estimator, MergePolicyType.TIER,
                recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);
        long speedLimit = solver.solveMaxSpeedProbSampling();
        Assert.assertTrue(speedLimit >= 46500 && speedLimit <= 46800);
    }

    @Test
    public void testInitializeNPE() {
        int toleratedComponents = 3;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(75609.960000, 2380.579000, 71456.000000, 82302.000000);
        double maxIoSpeed = 96379.414095;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.313000, 0.094000, 0.267000, 0.767000),
                RandomVariable.of(0.441000, 0.080000, 0.308000, 0.667000),
                RandomVariable.of(0.466000, 0.168000, 0.275000, 1.255000),
                RandomVariable.of(0.518000, 0.159000, 0.328000, 1.183000),
                RandomVariable.of(0.718000, 0.314000, 0.417000, 2.421000),
                RandomVariable.of(1.020000, 0.423000, 0.501000, 2.950000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.667000, 0.125000, 2.402000, 2.933000),
                RandomVariable.of(6.763000, 2.766000, 2.405000, 9.336000),
                RandomVariable.of(6.403000, 2.975000, 2.940000, 10.942000),
                RandomVariable.of(8.040000, 1.824000, 5.877000, 12.009000),
                RandomVariable.of(10.799000, 5.664000, 6.409000, 25.356000),
                RandomVariable.of(12.990000, 9.374000, 6.283000, 39.184000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.354000, 0.097000, 0.300000, 0.840000),
                        RandomVariable.of(0.497000, 0.220000, 0.333000, 1.696000),
                        RandomVariable.of(1.487000, 0.573000, 0.718000, 3.359000),
                        RandomVariable.of(2.171000, 0.658000, 1.002000, 3.784000),
                        RandomVariable.of(3.894000, 0.917000, 2.559000, 6.626000),
                        RandomVariable.of(4.701000, 1.521000, 1.926000, 8.447000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.847000, 0.273000, 0.359000, 1.701000),
                        RandomVariable.of(1.351000, 0.317000, 0.792000, 2.151000),
                        RandomVariable.of(2.416000, 0.638000, 1.152000, 4.676000),
                        RandomVariable.of(3.804000, 0.892000, 2.276000, 6.527000),
                        RandomVariable.of(4.156000, 1.249000, 2.344000, 8.241000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.882000, 0.247000, 0.358000, 1.700000),
                        RandomVariable.of(1.509000, 0.505000, 0.767000, 3.184000),
                        RandomVariable.of(2.458000, 0.651000, 1.525000, 4.784000),
                        RandomVariable.of(3.812000, 1.196000, 1.767000, 8.011000),
                        RandomVariable.of(4.453000, 0.962000, 2.142000, 6.377000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.729000, 0.056000, 0.616000, 0.968000),
                        RandomVariable.of(1.845000, 0.587000, 0.967000, 2.893000),
                        RandomVariable.of(2.541000, 0.567000, 1.131000, 4.336000),
                        RandomVariable.of(4.179000, 1.061000, 2.131000, 6.928000),
                        RandomVariable.of(4.526000, 1.123000, 1.969000, 7.461000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.729000, 0.056000, 0.616000, 0.968000),
                        RandomVariable.of(1.094000, 0.056000, 0.925000, 1.452000),
                        RandomVariable.of(3.012000, 0.431000, 2.667000, 4.050000),
                        RandomVariable.of(4.187000, 0.980000, 1.583000, 7.659000),
                        RandomVariable.of(4.861000, 0.868000, 2.534000, 7.548000) } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(2.015000, 0.144000, 1.780000, 2.136000),
                        RandomVariable.of(2.403000, 0.127000, 2.313000, 2.493000),
                        RandomVariable.of(4.699000, 1.943000, 1.961000, 7.834000),
                        RandomVariable.of(6.183000, 2.272000, 3.739000, 9.791000),
                        RandomVariable.of(9.017000, 4.498000, 3.028000, 17.789000),
                        RandomVariable.of(5.516000, 1.005000, 4.805000, 6.227000) },
                { null, RandomVariable.of(1.166000, 0.138000, 1.068000, 1.263000),
                        RandomVariable.of(2.732000, 0.815000, 1.554000, 3.803000),
                        RandomVariable.of(3.204000, 1.863000, 1.557000, 5.626000),
                        RandomVariable.of(3.629000, 1.749000, 1.949000, 7.665000),
                        RandomVariable.of(4.998000, 2.687000, 1.748000, 8.737000),
                        RandomVariable.of(8.558000, 3.731000, 5.048000, 14.459000) },
                { null, RandomVariable.of(1.166000, 0.138000, 1.068000, 1.263000),
                        RandomVariable.of(0.986000, 0.000000, 0.986000, 0.986000),
                        RandomVariable.of(1.560000, 0.000000, 1.560000, 1.560000),
                        RandomVariable.of(1.555000, 0.700000, 0.862000, 2.833000),
                        RandomVariable.of(2.706000, 0.709000, 1.621000, 3.559000),
                        RandomVariable.of(3.133000, 0.601000, 2.751000, 3.826000) },
                { null, RandomVariable.of(1.166000, 0.138000, 1.068000, 1.263000),
                        RandomVariable.of(2.331000, 0.138000, 2.136000, 2.527000),
                        RandomVariable.of(3.497000, 0.138000, 3.204000, 3.790000),
                        RandomVariable.of(1.266000, 0.000000, 1.266000, 1.266000),
                        RandomVariable.of(0.979000, 0.189000, 0.809000, 1.183000),
                        RandomVariable.of(1.175000, 0.189000, 0.971000, 1.419000) },
                { null, RandomVariable.of(1.166000, 0.138000, 1.068000, 1.263000),
                        RandomVariable.of(2.331000, 0.138000, 2.136000, 2.527000),
                        RandomVariable.of(3.497000, 0.138000, 3.204000, 3.790000),
                        RandomVariable.of(1.266000, 0.000000, 1.266000, 1.266000),
                        RandomVariable.of(0.979000, 0.189000, 0.809000, 1.183000),
                        RandomVariable.of(1.175000, 0.189000, 0.971000, 1.419000) } };
        RandomVariable[][] componentRatios = { { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = { 77546.0, 77488.0, 74240.0, 78996.0 };
        double initialCurrentUsedMemory = 0.000000;
        double initialFlushCapacity = 77546.000000;
        double initialFlushFinalizedPages = 4.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { { 76038.0, 74530.0, 74530.0, 74530.0, 78880.0, 74878.0 },
                { 223648.0, 232058.0, 231942.0, 220458.0, 226200.0 },
                { 693042.0, 684400.0, 678658.0, 683994.0, 676222.0 }, { 2076342.0, 2095830.0, 2074718.0, 2108010.0 },
                { 6219282.0, 6260984.0, 6269162.0, 6161862.0 } };
        double[] initialMergedComponents = { 225098.0, 664576.0, 1812244.0, 3050772.0, 9091428.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes =
                { 0.515390805, 1.379284368, 2.976009253, 1.407109857, 2.976060306 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.618321;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.618321, 7271.696843, 0.010000, 131072);
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, 5, memoryComponentCapacity,
                numMemoryComponents, sizeRatio, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes,
                mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory,
                initialFlushCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponents, initialMergedComponents, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes, componentRatios, estimator, MergePolicyType.TIER,
                recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);

        long limit = solver.solveMaxSpeedProbSampling();
        Assert.assertTrue(limit >= 6750 && limit <= 6800);
    }

    @Test
    public void testInitializeNPE2() {
        int toleratedComponents = 3;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(76345.400000, 3264.619000, 69194.000000, 85086.000000);
        double maxIoSpeed = 96385.527367;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.313000, 0.094000, 0.267000, 0.767000),
                RandomVariable.of(0.441000, 0.080000, 0.308000, 0.667000),
                RandomVariable.of(0.466000, 0.168000, 0.275000, 1.255000),
                RandomVariable.of(0.562000, 0.361000, 0.328000, 2.714000),
                RandomVariable.of(0.717000, 0.193000, 0.393000, 1.257000),
                RandomVariable.of(0.860000, 0.193000, 0.472000, 1.509000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.667000, 0.125000, 2.402000, 2.933000),
                RandomVariable.of(6.763000, 2.766000, 2.405000, 9.336000),
                RandomVariable.of(6.403000, 2.975000, 2.940000, 10.942000),
                RandomVariable.of(8.756000, 3.755000, 5.074000, 15.212000),
                RandomVariable.of(13.400000, 6.087000, 6.142000, 22.783000),
                RandomVariable.of(16.079000, 6.087000, 7.370000, 27.339000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.354000, 0.097000, 0.300000, 0.840000),
                        RandomVariable.of(0.497000, 0.220000, 0.333000, 1.696000),
                        RandomVariable.of(1.342000, 0.363000, 0.718000, 2.209000),
                        RandomVariable.of(2.260000, 0.559000, 1.375000, 3.605000),
                        RandomVariable.of(3.098000, 0.952000, 1.614000, 5.832000),
                        RandomVariable.of(3.718000, 0.952000, 1.937000, 6.998000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.847000, 0.273000, 0.359000, 1.701000),
                        RandomVariable.of(1.333000, 0.304000, 0.792000, 2.151000),
                        RandomVariable.of(2.453000, 0.699000, 1.267000, 4.984000),
                        RandomVariable.of(2.710000, 0.620000, 1.413000, 4.205000),
                        RandomVariable.of(3.252000, 0.620000, 1.696000, 5.046000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.882000, 0.247000, 0.358000, 1.700000),
                        RandomVariable.of(1.381000, 0.370000, 0.767000, 2.259000),
                        RandomVariable.of(2.498000, 0.688000, 1.550000, 5.059000),
                        RandomVariable.of(3.014000, 0.711000, 1.768000, 5.201000),
                        RandomVariable.of(3.617000, 0.711000, 2.121000, 6.241000) },
                { null, RandomVariable.of(0.365000, 0.056000, 0.308000, 0.484000),
                        RandomVariable.of(0.729000, 0.056000, 0.616000, 0.968000),
                        RandomVariable.of(1.494000, 0.264000, 1.058000, 1.792000),
                        RandomVariable.of(2.459000, 0.686000, 1.475000, 4.443000),
                        RandomVariable.of(2.927000, 0.707000, 1.901000, 5.311000),
                        RandomVariable.of(3.513000, 0.707000, 2.282000, 6.374000) },
                { null, null, null, null, null, null, null } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(2.015000, 0.144000, 1.780000, 2.136000),
                        RandomVariable.of(2.403000, 0.127000, 2.313000, 2.493000),
                        RandomVariable.of(4.182000, 1.774000, 1.961000, 7.834000),
                        RandomVariable.of(6.390000, 3.650000, 2.673000, 13.698000),
                        RandomVariable.of(7.987000, 3.650000, 3.341000, 17.122000),
                        RandomVariable.of(9.585000, 3.650000, 4.009000, 20.546000) },
                { null, RandomVariable.of(1.166000, 0.138000, 1.068000, 1.263000),
                        RandomVariable.of(2.732000, 0.815000, 1.554000, 3.803000),
                        RandomVariable.of(3.624000, 2.035000, 1.557000, 5.626000),
                        RandomVariable.of(3.490000, 1.380000, 1.912000, 5.509000),
                        RandomVariable.of(4.004000, 1.833000, 1.748000, 5.726000),
                        RandomVariable.of(4.805000, 1.833000, 2.098000, 6.872000) },
                { null, RandomVariable.of(1.166000, 0.138000, 1.068000, 1.263000),
                        RandomVariable.of(0.986000, 0.000000, 0.986000, 0.986000),
                        RandomVariable.of(1.560000, 0.000000, 1.560000, 1.560000),
                        RandomVariable.of(1.096000, 0.246000, 0.862000, 1.352000),
                        RandomVariable.of(2.601000, 0.969000, 1.621000, 3.559000),
                        RandomVariable.of(3.122000, 0.969000, 1.945000, 4.271000) },
                { RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000),
                        RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000),
                        RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000),
                        RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000),
                        RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000),
                        RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000),
                        RandomVariable.of(0.945000, 0.000000, 0.945000, 0.945000) },
                { null, null, null, null, null, null, null } };
        RandomVariable[][] componentRatios = { { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = { 82012.0, 78010.0, 76270.0, 73312.0 };
        double initialCurrentUsedMemory = 0.000000;
        double initialFlushCapacity = 82012.000000;
        double initialFlushFinalizedPages = 4.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { { 73776.0, 72964.0, 74820.0, 73370.0, 77140.0, 77952.0 },
                { 227708.0, 225330.0 }, { 677846.0, 681848.0, 699480.0, 681268.0, 669958.0, 701742.0 },
                { 2040498.0, 2110156.0, 2110330.0, 2108416.0, 2101572.0 }, { 6219282.0 } };
        double[] initialMergedComponents = { 221560.0, 0.0, 1721620.0, 5678396.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes =
                { 0.24412838099999998, 0.0, 0.7446233059999999, 0.402684328, 0.0 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.633266;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.633266, 7271.785272, 0.010000, 131072);
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, 5, memoryComponentCapacity,
                numMemoryComponents, sizeRatio, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes,
                mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory,
                initialFlushCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponents, initialMergedComponents, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes, componentRatios, estimator, MergePolicyType.TIER,
                recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);
        long limit = solver.solveMaxSpeedProbSampling();
        Assert.assertTrue(limit >= 1450 && limit <= 1500);
    }

}
