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

public class TierFlowControlSpeedSolverSlow {
    @Test
    public void test() {
        int toleratedComponents = 3;
        int numMemoryComponents = 4;
        RandomVariable memoryComponentCapacity =
                RandomVariable.of(75797.880000, 2474.115000, 71456.000000, 82302.000000);
        double maxIoSpeed = 81844.995181;
        RandomVariable[] flushProcessingTimes = { null, RandomVariable.of(0.369000, 0.070000, 0.299000, 0.516000),
                RandomVariable.of(0.558000, 0.224000, 0.329000, 1.804000),
                RandomVariable.of(0.505000, 0.147000, 0.343000, 0.989000),
                RandomVariable.of(0.647000, 0.293000, 0.367000, 2.242000),
                RandomVariable.of(0.782000, 0.340000, 0.483000, 2.149000),
                RandomVariable.of(1.056000, 0.323000, 0.562000, 2.190000),
                RandomVariable.of(1.232000, 0.323000, 0.656000, 2.555000) };
        RandomVariable[] flushFinalizeTimes = { null, RandomVariable.of(3.552000, 1.600000, 2.361000, 6.668000),
                RandomVariable.of(7.746000, 3.752000, 3.204000, 15.187000),
                RandomVariable.of(5.837000, 2.741000, 4.008000, 13.079000),
                RandomVariable.of(14.010000, 9.582000, 4.008000, 38.095000),
                RandomVariable.of(14.045000, 5.860000, 6.945000, 22.952000),
                RandomVariable.of(17.964000, 13.037000, 8.810000, 53.393000),
                RandomVariable.of(20.958000, 13.037000, 10.278000, 62.292000) };
        RandomVariable[][] mergeProcessingTimes = {
                { null, RandomVariable.of(0.519000, 0.393000, 0.333000, 2.222000),
                        RandomVariable.of(0.615000, 0.299000, 0.384000, 2.317000),
                        RandomVariable.of(1.650000, 1.051000, 0.542000, 6.969000),
                        RandomVariable.of(2.029000, 0.987000, 0.458000, 6.345000),
                        RandomVariable.of(2.356000, 0.928000, 0.805000, 4.494000),
                        RandomVariable.of(2.658000, 1.183000, 0.676000, 5.561000),
                        RandomVariable.of(3.101000, 1.183000, 0.788000, 6.488000) },
                { null, RandomVariable.of(0.429000, 0.111000, 0.333000, 0.675000),
                        RandomVariable.of(1.106000, 0.568000, 0.442000, 3.685000),
                        RandomVariable.of(2.101000, 0.816000, 0.992000, 4.710000),
                        RandomVariable.of(2.704000, 0.979000, 0.825000, 4.668000),
                        RandomVariable.of(3.427000, 0.975000, 1.471000, 6.603000),
                        RandomVariable.of(3.672000, 0.815000, 1.941000, 5.971000),
                        RandomVariable.of(3.101000, 1.183000, 0.788000, 6.488000) },
                { null, RandomVariable.of(0.579000, 0.172000, 0.467000, 0.992000),
                        RandomVariable.of(1.187000, 0.359000, 0.667000, 2.152000),
                        RandomVariable.of(2.731000, 0.969000, 1.233000, 5.927000),
                        RandomVariable.of(3.344000, 0.733000, 2.009000, 4.754000),
                        RandomVariable.of(3.806000, 1.031000, 1.951000, 6.012000),
                        RandomVariable.of(4.270000, 0.864000, 2.592000, 5.978000),
                        RandomVariable.of(3.101000, 1.183000, 0.788000, 6.488000) },
                { null, RandomVariable.of(0.594000, 0.032000, 0.571000, 0.617000),
                        RandomVariable.of(1.270000, 0.304000, 0.712000, 2.084000),
                        RandomVariable.of(2.617000, 0.755000, 1.084000, 4.251000),
                        RandomVariable.of(3.478000, 0.904000, 2.142000, 6.653000),
                        RandomVariable.of(3.811000, 0.847000, 2.123000, 5.447000),
                        RandomVariable.of(4.281000, 0.887000, 2.951000, 6.325000),
                        RandomVariable.of(3.101000, 1.183000, 0.788000, 6.488000) },
                { null, RandomVariable.of(0.535000, 0.031000, 0.492000, 0.658000),
                        RandomVariable.of(1.154000, 0.341000, 0.667000, 2.493000),
                        RandomVariable.of(2.767000, 0.781000, 1.217000, 4.751000),
                        RandomVariable.of(3.417000, 0.706000, 2.174000, 4.522000),
                        RandomVariable.of(4.113000, 0.990000, 1.643000, 6.536000),
                        RandomVariable.of(4.393000, 0.821000, 2.570000, 5.978000),
                        RandomVariable.of(3.101000, 1.183000, 0.788000, 6.488000) },
                { null, null, null, null, null, null, null, null } };
        RandomVariable[][] mergeFinalizeTimes = {
                { null, RandomVariable.of(2.995000, 1.337000, 1.780000, 4.453000),
                        RandomVariable.of(3.330000, 0.811000, 2.133000, 3.917000),
                        RandomVariable.of(4.699000, 1.565000, 2.319000, 7.295000),
                        RandomVariable.of(7.032000, 2.944000, 3.563000, 11.301000),
                        RandomVariable.of(9.267000, 3.375000, 5.339000, 13.881000),
                        RandomVariable.of(15.455000, 11.940000, 4.633000, 47.503000),
                        RandomVariable.of(18.030000, 11.940000, 5.405000, 55.420000) },
                { null, RandomVariable.of(2.995000, 1.337000, 1.780000, 4.453000),
                        RandomVariable.of(2.263000, 0.791000, 1.460000, 3.397000),
                        RandomVariable.of(2.641000, 1.350000, 1.554000, 5.725000),
                        RandomVariable.of(4.034000, 2.107000, 2.331000, 8.454000),
                        RandomVariable.of(7.641000, 3.039000, 3.495000, 14.028000),
                        RandomVariable.of(8.901000, 2.717000, 5.767000, 10.588000),
                        RandomVariable.of(18.030000, 11.940000, 5.405000, 55.420000) },
                { null, RandomVariable.of(0.591000, 0.000000, 0.591000, 0.591000),
                        RandomVariable.of(1.396000, 0.812000, 0.822000, 1.971000),
                        RandomVariable.of(1.685000, 0.656000, 1.028000, 2.341000),
                        RandomVariable.of(1.898000, 0.911000, 1.028000, 3.448000),
                        RandomVariable.of(2.267000, 1.353000, 1.111000, 3.755000),
                        RandomVariable.of(8.901000, 2.717000, 5.767000, 10.588000),
                        RandomVariable.of(18.030000, 11.940000, 5.405000, 55.420000) },
                { null, RandomVariable.of(0.381000, 0.000000, 0.381000, 0.381000),
                        RandomVariable.of(0.641000, 0.000000, 0.641000, 0.641000),
                        RandomVariable.of(1.685000, 0.656000, 1.028000, 2.341000),
                        RandomVariable.of(0.804000, 0.218000, 0.650000, 0.958000),
                        RandomVariable.of(1.660000, 0.839000, 1.067000, 2.254000),
                        RandomVariable.of(8.901000, 2.717000, 5.767000, 10.588000),
                        RandomVariable.of(18.030000, 11.940000, 5.405000, 55.420000) },
                { null, RandomVariable.of(0.386000, 0.417000, 0.092000, 0.681000),
                        RandomVariable.of(0.656000, 0.704000, 0.158000, 1.154000),
                        RandomVariable.of(1.685000, 0.656000, 1.028000, 2.341000),
                        RandomVariable.of(0.804000, 0.218000, 0.650000, 0.958000),
                        RandomVariable.of(1.660000, 0.839000, 1.067000, 2.254000),
                        RandomVariable.of(8.901000, 2.717000, 5.767000, 10.588000),
                        RandomVariable.of(18.030000, 11.940000, 5.405000, 55.420000) },
                { null, null, null, null, null, null, null, null } };
        RandomVariable[][] componentRatios = { { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) },
                { RandomVariable.of(1.000000, 0.000000, 1.000000, 1.000000) } };
        double[] initialUsedMemory = {};
        double initialCurrentUsedMemory = 0.000000;
        double initialFlushCapacity = 0.000000;
        double initialFlushFinalizedPages = 0.000000;
        double initialFlushSubOperationElapsedTime = 0.000000;
        double[][] initialComponents = { {}, {}, {}, {}, {}, {} };
        double[] initialMergedComponents = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
        double[] initialMergeFinalizedPages = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
        double[] initialMergeSubOperationElapsedTimes = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
        double subOperationPages = 128.000000;
        double baseLevelCapacity = 68962.000000;
        double recordsPerPage = 235.660134;
        ILSMFinalizingPagesEstimator estimator =
                new LSMFinalizingPagesEstimator(235.660134, 7271.719262, 0.010000, 131072);
        FlowControlSpeedSolver solver =
                new FlowControlSpeedSolver(toleratedComponents, 6, memoryComponentCapacity, numMemoryComponents, 3,
                        maxIoSpeed, flushProcessingTimes, flushFinalizeTimes, mergeProcessingTimes, mergeFinalizeTimes,
                        initialUsedMemory, initialCurrentUsedMemory, initialFlushCapacity, initialFlushFinalizedPages,
                        initialFlushSubOperationElapsedTime, initialComponents, initialMergedComponents,
                        initialMergeFinalizedPages, initialMergeSubOperationElapsedTimes, componentRatios, estimator,
                        MergePolicyType.TIER, recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();
        solver.solveMaxSpeedProbSampling();

    }
}
