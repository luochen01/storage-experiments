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

import org.junit.Test;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.FlowControlSpeedSolver.MergePolicyType;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.ILSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.LSMFinalizingPagesEstimator;
import edu.uci.asterixdb.storage.experiments.lsm.simulator.RandomVariable;

public class LevelFlowControlSpeedSolverSlow {
    @Test
    public void test() {
        int toleratedComponents = 1;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(78007.680000, 2949.969000, 72384.000000, 83578.000000);
        double maxIoSpeed = 102879.837629;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.293000, 0.048000, 0.257000, 0.535000),
                RandomVariable.of(0.378000, 0.077000, 0.286000, 0.683000),
                RandomVariable.of(0.528000, 0.182000, 0.277000, 0.977000),
                RandomVariable.of(0.704000, 0.182000, 0.370000, 1.303000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(2.940000, 0.126000, 2.673000, 3.207000),
                RandomVariable.of(4.871000, 2.569000, 2.944000, 9.877000),
                RandomVariable.of(7.246000, 4.693000, 3.209000, 18.414000),
                RandomVariable.of(9.662000, 4.693000, 4.278000, 24.552000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.333000, 0.029000, 0.299000, 0.425000),
                        RandomVariable.of(0.761000, 0.192000, 0.367000, 1.406000),
                        RandomVariable.of(1.217000, 0.450000, 0.401000, 2.484000),
                        RandomVariable.of(1.623000, 0.450000, 0.535000, 3.312000) },
                { null, RandomVariable.of(0.433000, 0.104000, 0.350000, 1.018000),
                        RandomVariable.of(0.772000, 0.169000, 0.393000, 1.150000),
                        RandomVariable.of(1.201000, 0.333000, 0.767000, 2.139000),
                        RandomVariable.of(1.602000, 0.333000, 1.023000, 2.853000) },
                { null, RandomVariable.of(0.457000, 0.026000, 0.408000, 0.517000),
                        RandomVariable.of(0.862000, 0.199000, 0.467000, 1.276000),
                        RandomVariable.of(1.453000, 0.439000, 0.950000, 2.385000),
                        RandomVariable.of(1.937000, 0.439000, 1.267000, 3.180000) } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(1.696000, 0.581000, 1.137000, 2.849000),
                        RandomVariable.of(3.271000, 1.395000, 1.738000, 6.051000),
                        RandomVariable.of(5.498000, 2.995000, 2.780000, 10.541000),
                        RandomVariable.of(7.331000, 2.995000, 3.707000, 14.055000) },
                { null, RandomVariable.of(0.673000, 0.274000, 0.301000, 1.191000),
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
        double initialCurrentUsedMemory = 0;
        double initialFlushCapacity = 0.000000;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { {}, {}, {} };
        double[] initialMergedComponents = { 0.0, 0.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes = { 0.0, 0.0, 0.0 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.628538;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.628538, 7272.001590, 0.010000, 131072);
        FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, 3, memoryComponentCapacity,
                numMemoryComponents, 10, maxIoSpeed, flushProcessingTimes, flushFinalizeTimes, mergeProcessingTimes,
                mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory, initialFlushCapacity,
                initialFlushFinalizedPages, initialFlushSubOperationElapsedTime, initialComponents,
                initialMergedComponents, initialMergeFinalizedPages, initialMergeSubOperationElapsedTimes,
                componentRatios, estimator, MergePolicyType.LEVEL, recordsPerPage * subOperationPages,
                subOperationPages, baseLevelCapacity);
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
    }
}
