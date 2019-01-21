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
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlowControlSpeedSolver {

    public static final boolean VERBOSE = false;

    public enum MergePolicyType {
        LEVEL,
        TIER
    }

    public enum PartitionPolicy {
        Blocking,
        Dual,
        DualSelect,
        NonBlocking,
        Hybrid,
    }

    private static final Logger LOGGER = LogManager.getLogger(FlowControlSpeedSolver.class);

    protected final double maxIoSpeed;
    protected final IOperationScheduler scheduler;

    protected final double[] initialMemoryComponentCapacities;
    protected final double initialCurrentMemoryComponentCapacity;

    protected final double initialFlushedCapacity;
    protected final double initialFlushFinalizedPages;
    protected final double initialFlushSubOperationElapsedTime;

    protected final double[][] initialComponentsCapacities;
    protected final double[] initialMergedCapacities;
    protected final double[] initialMergeFinalizedPages;
    protected final double[] initialMergeSubOperationElapsedTimes;

    // one day
    public static double MAX_TIME = 6 * 3600;

    private static int MAX_SOL_SIZE = 1000;

    public static double PROB_RATE = 0.6;

    public static double STOP_RANGE = 50;
    public static double STOP_PROB = 0.95;

    public static double MAX_SAMPLE_SIZE = 10;

    public FlowControlSpeedSolver(int toleratedComponentsPerLevel, int numLevels,
            RandomVariable memoryComponentCapacity, int totalMemoryComponents, int sizeRatio, double maxIoSpeed,
            RandomVariable[] flushProcessingSpeeds, RandomVariable[] flushFinalizeSpeeds,
            RandomVariable[][] mergeProcessingSpeeds, RandomVariable[][] mergeFinalizeSpeeds,
            double[] memoryComponentCapacities, double currentMemoryComponentCapacity, double flushedCapacity,
            double flushFinalizedPages, double flushSubOperationElapsedTime, double[][] usedComponentsCapacities,
            double[] mergedCapacities, double[] mergeFinalizedPages, double[] mergeSubOperationElapsedTimes,
            RandomVariable[][] mergeComponentRatios, ILSMFinalizingPagesEstimator pageEstimator, MergePolicyType type,
            double subOperationProcessingRecords, double subOperationPages, double baseLevelCapacity) {
        this(toleratedComponentsPerLevel, numLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio,
                maxIoSpeed, flushProcessingSpeeds, flushFinalizeSpeeds, mergeProcessingSpeeds, mergeFinalizeSpeeds,
                memoryComponentCapacities, currentMemoryComponentCapacity, flushedCapacity, flushFinalizedPages,
                flushSubOperationElapsedTime, usedComponentsCapacities, mergedCapacities, mergeFinalizedPages,
                mergeSubOperationElapsedTimes, mergeComponentRatios, pageEstimator, type, subOperationProcessingRecords,
                subOperationPages, baseLevelCapacity, false, 0, 0, 0, null, 0, 0);
    }

    public FlowControlSpeedSolver(int toleratedComponentsPerLevel, int numLevels,
            RandomVariable memoryComponentCapacity, int totalMemoryComponents, int sizeRatio, double maxIoSpeed,
            RandomVariable[] flushProcessingSpeeds, RandomVariable[] flushFinalizeSpeeds,
            RandomVariable[][] mergeProcessingSpeeds, RandomVariable[][] mergeFinalizeSpeeds,
            double[] memoryComponentCapacities, double currentMemoryComponentCapacity, double flushedCapacity,
            double flushFinalizedPages, double flushSubOperationElapsedTime, double[][] usedComponentsCapacities,
            double[] mergedCapacities, double[] mergeFinalizedPages, double[] mergeSubOperationElapsedTimes,
            RandomVariable[][] mergeComponentRatios, ILSMFinalizingPagesEstimator pageEstimator, MergePolicyType type,
            double subOperationProcessingRecords, double subOperationPages, double baseLevelCapacity,
            boolean partitioned, int sizeRatioLevel0, int toleratedComponentsLevel0, double diskComponentCapacity,
            PartitionPolicy partitionPolicy, int maxNumStackedComponents, double level1Capacity) {
        int numEffectiveLevels = getNumEffectiveLevels(numLevels, mergeProcessingSpeeds, mergeFinalizeSpeeds);
        this.maxIoSpeed = maxIoSpeed;
        ISpeedProvider flushSpeed = new SpeedProvider(flushProcessingSpeeds, flushFinalizeSpeeds);
        ISpeedProvider[] mergeSpeeds = new SpeedProvider[numEffectiveLevels];
        for (int i = 0; i < numEffectiveLevels; i++) {
            mergeSpeeds[i] = new SpeedProvider(mergeProcessingSpeeds[i], mergeFinalizeSpeeds[i]);
        }
        if (partitioned) {
            if (type != MergePolicyType.LEVEL) {
                throw new IllegalArgumentException("Partitioned tiering policy is not supported");
            }
            switch (partitionPolicy) {
                case Blocking:
                    this.scheduler = new BlockingPartitionedLevelMergeScheduler(toleratedComponentsPerLevel,
                            numEffectiveLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio, flushSpeed,
                            mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                            baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                            diskComponentCapacity, level1Capacity);
                    break;
                case Dual:
                    this.scheduler = new DualPartitionedLevelMergeScheduler(toleratedComponentsPerLevel,
                            numEffectiveLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio, flushSpeed,
                            mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                            baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                            diskComponentCapacity, level1Capacity);
                    break;
                case DualSelect:
                    this.scheduler = new DualSelectPartitionedLevelMergeScheduler(toleratedComponentsPerLevel,
                            numEffectiveLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio, flushSpeed,
                            mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                            baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                            diskComponentCapacity, level1Capacity);
                    break;
                case NonBlocking:
                    this.scheduler = new NonBlockingPartitionedLevelMergeScheduler(toleratedComponentsPerLevel,
                            numEffectiveLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio, flushSpeed,
                            mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                            baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                            diskComponentCapacity);
                    break;
                case Hybrid:
                    this.scheduler = new HybridPartitionedLevelMergeScheduler(toleratedComponentsPerLevel,
                            numEffectiveLevels, memoryComponentCapacity, totalMemoryComponents, sizeRatio, flushSpeed,
                            mergeSpeeds, pageEstimator, subOperationProcessingRecords, subOperationPages,
                            baseLevelCapacity, mergeComponentRatios, sizeRatioLevel0, toleratedComponentsLevel0,
                            diskComponentCapacity, maxNumStackedComponents);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown partition policy " + partitionPolicy);
            }

        } else {
            this.scheduler = type == MergePolicyType.LEVEL
                    ? new LevelMergeScheduler(toleratedComponentsPerLevel, numEffectiveLevels, memoryComponentCapacity,
                            totalMemoryComponents, sizeRatio, flushSpeed, mergeSpeeds, pageEstimator,
                            subOperationProcessingRecords, subOperationPages, baseLevelCapacity, mergeComponentRatios)
                    : new TierMergeScheduler(toleratedComponentsPerLevel, numEffectiveLevels, memoryComponentCapacity,
                            totalMemoryComponents, sizeRatio, flushSpeed, mergeSpeeds, pageEstimator,
                            subOperationProcessingRecords, subOperationPages, baseLevelCapacity, mergeComponentRatios);
        }

        this.scheduler.initialize();
        this.initialMemoryComponentCapacities = memoryComponentCapacities;
        this.initialCurrentMemoryComponentCapacity = currentMemoryComponentCapacity;
        this.initialFlushedCapacity = flushedCapacity;
        this.initialFlushFinalizedPages = flushFinalizedPages;
        this.initialFlushSubOperationElapsedTime = flushSubOperationElapsedTime;

        this.initialComponentsCapacities = usedComponentsCapacities;
        this.initialMergedCapacities = mergedCapacities;
        this.initialMergeFinalizedPages = mergeFinalizedPages;
        this.initialMergeSubOperationElapsedTimes = mergeSubOperationElapsedTimes;

        if (VERBOSE) {
            LOGGER.error("size ratio: {}", sizeRatio);
            LOGGER.error("tolerated components per level: {}", toleratedComponentsPerLevel);
            LOGGER.error("max io speed: {}", maxIoSpeed);
            LOGGER.error("memory component capacity: {}", memoryComponentCapacity);
            LOGGER.error("num memory components: {}", totalMemoryComponents);
            LOGGER.error("flush processing times: {}", Arrays.toString(flushProcessingSpeeds));
            LOGGER.error("flush finalize times: {}", Arrays.toString(flushFinalizeSpeeds));
            LOGGER.error("merge processing times: \n {}", toString(mergeProcessingSpeeds));
            LOGGER.error("merge finalize times: \n {}", toString(mergeFinalizeSpeeds));
            LOGGER.error("merge component ratios: {}", toString(mergeComponentRatios));

            LOGGER.error("inital used memory: {}", Arrays.toString(initialMemoryComponentCapacities));
            LOGGER.error("initial current used memory: {}", initialCurrentMemoryComponentCapacity);
            LOGGER.error("initial flush capacity: {}", initialFlushedCapacity);
            LOGGER.error("initial flush finalized pages: {}", initialFlushFinalizedPages);
            LOGGER.error("initial flush sub operation elapsed time: {}", initialFlushSubOperationElapsedTime);

            LOGGER.error("initial merge components: {}", toString(initialComponentsCapacities));
            LOGGER.error("initial merged components: {}", initialMergedCapacities);
            LOGGER.error("initial merge finalized pages: {}", initialMergeFinalizedPages);
            LOGGER.error("initial merge sub operation elapsed times: {}", initialMergeSubOperationElapsedTimes);
            LOGGER.error("sub operation pages: {}", subOperationPages);
            LOGGER.error("base level capacity: {}", baseLevelCapacity);
            LOGGER.error("page estimator: {}", pageEstimator);
        }
    }

    private String toString(RandomVariable[][] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length - 1; i++) {
            sb.append(Arrays.toString(values[i]));
            sb.append(",\n");
        }
        sb.append(Arrays.toString(values[values.length - 1]));
        sb.append("]");
        return sb.toString();
    }

    private String toString(double[][] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length - 1; i++) {
            sb.append(Arrays.toString(values[i]));
            sb.append(",\n");
        }
        sb.append(Arrays.toString(values[values.length - 1]));
        sb.append("]");
        return sb.toString();
    }

    private int getNumEffectiveLevels(int numLevels, RandomVariable[][] mergeProcessingSpeeds,
            RandomVariable[][] mergeFinalizeSpeeds) {
        for (int i = 0; i < numLevels; i++) {
            if (mergeFinalizeSpeeds[i][1] == null || DoubleUtil.equals(mergeFinalizeSpeeds[i][1].mean, 0.0)) {
                return i;
            }
        }
        return numLevels;
    }

    public int solveMaxSpeed() {
        long begin = System.nanoTime();

        // perform a binary search between lastSpeed and currentSpeed
        int low = 0;
        int high = (int) maxIoSpeed * 2;
        boolean changed = true;
        int steps = 0;
        while (changed) {
            changed = false;
            int mid = (low + high) >>> 1;
            boolean midVal = simulate(mid, true);
            steps++;
            if (midVal) {
                int newHigh = mid;
                if (high != newHigh) {
                    changed = true;
                    high = newHigh;
                }
            } else {
                int newLow = mid;
                if (low != newLow) {
                    changed = true;
                    low = newLow;
                }
            }
            if (high - low <= 100) {
                low = (low + high) / 2;
                break;
            }
        }
        //assert !simulate(low, true, null, null);
        long end = System.nanoTime();
        if (VERBOSE) {
            LOGGER.error("Finding max speed {} takes {} ms in {} iterations", low,
                    TimeUnit.NANOSECONDS.toMillis(end - begin), steps);
        }
        return low;
    }

    public int solveMaxBlockingSpeed() {
        MutableDouble data = new MutableDouble();
        scheduler.prepare(maxIoSpeed, initialMemoryComponentCapacities, initialCurrentMemoryComponentCapacity,
                initialFlushedCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponentsCapacities, initialMergedCapacities, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes);
        scheduler.simulate(maxIoSpeed, MAX_TIME, data, false);
        return (int) (data.getValue() / MAX_TIME);
    }

    public int solveMaxSpeedProb(MutableInt iterations) {
        long begin = System.nanoTime();
        // perform a binary search between lastSpeed and currentSpeed
        int high = (int) maxIoSpeed;
        double[] ranges = new double[MAX_SOL_SIZE];
        double[] pdf = new double[MAX_SOL_SIZE];
        double[] nextRanges = new double[MAX_SOL_SIZE];
        double[] nextPdf = new double[MAX_SOL_SIZE];

        ranges[0] = high;
        pdf[0] = 1;
        int length = 1;
        while (true) {
            double x = getNextProbeSpeed(ranges, pdf, length);
            boolean stalled = simulate(x, true);
            if (stalled) {
                // treat as positive sign
                // values > x should have less confidence; while value < x should have larger confidence
                computeNext(ranges, pdf, nextRanges, nextPdf, x, length, PROB_RATE);
            } else {
                computeNext(ranges, pdf, nextRanges, nextPdf, x, length, 1 - PROB_RATE);
            }
            length++;

            // check whether we can stop at here
            double[] tmpRanges = ranges;
            ranges = nextRanges;
            nextRanges = tmpRanges;
            double[] tmpPdf = pdf;
            pdf = nextPdf;
            nextPdf = tmpPdf;

            assert sanityCheck(pdf, length);

            // check whether we can stop
            for (int i = 0; i < length; i++) {
                double startRange = i > 0 ? ranges[i - 1] : 0;
                double prevRange = startRange;
                double runningRange = 0;
                double runningProb = 0;
                for (int j = i; j < length; j++) {
                    runningRange += (ranges[i] - prevRange);
                    runningProb += pdf[i];
                    prevRange = ranges[i];
                    if (runningRange <= STOP_RANGE && runningProb >= STOP_PROB) {
                        if (iterations != null) {
                            iterations.setValue(length);
                        }
                        int speed = (int) startRange;
                        long end = System.nanoTime();
                        LOGGER.error("Finding max speed prob {} takes {} ms in {} iterations", speed,
                                TimeUnit.NANOSECONDS.toMillis(end - begin), length);
                        return speed;
                    } else if (runningRange > STOP_RANGE) {
                        break;
                    }
                }
            }
        }
    }

    public int solveMaxSpeedProbSampling() {
        return solveMaxSpeedProbSampling(0.5, 0.8, null);
    }

    public int solveMaxSpeedProbSampling(double stallProb, double sampleConfidence, MutableInt iterations) {
        long begin = System.nanoTime();

        // perform a binary search between lastSpeed and currentSpeed
        int high = (int) maxIoSpeed;
        double[] ranges = new double[MAX_SOL_SIZE];
        double[] pdf = new double[MAX_SOL_SIZE];
        double[] nextRanges = new double[MAX_SOL_SIZE];
        double[] nextPdf = new double[MAX_SOL_SIZE];
        int evaluations = 0;
        ranges[0] = high;
        pdf[0] = 1;
        int length = 1;
        while (true) {
            double x = getNextProbeSpeed(ranges, pdf, length);
            Triple<Boolean, Boolean, Integer> pair = sample(x, stallProb, sampleConfidence);
            boolean stalled = pair.getLeft();
            double rate = pair.getMiddle() ? sampleConfidence : PROB_RATE;
            if (iterations != null) {
                iterations.add(pair.getRight());
            }
            evaluations += pair.getRight();
            if (stalled) {
                // treat as positive sign
                // values > x should have less confidence; while value < x should have larger confidence
                computeNext(ranges, pdf, nextRanges, nextPdf, x, length, rate);
            } else {
                computeNext(ranges, pdf, nextRanges, nextPdf, x, length, 1 - rate);
            }
            length++;

            // check whether we can stop at here
            double[] tmpRanges = ranges;
            ranges = nextRanges;
            nextRanges = tmpRanges;
            double[] tmpPdf = pdf;
            pdf = nextPdf;
            nextPdf = tmpPdf;

            assert sanityCheck(pdf, length);

            // check whether we can stop
            for (int i = 0; i < length; i++) {
                double startRange = i > 0 ? ranges[i - 1] : 0;
                double prevRange = startRange;
                double runningRange = 0;
                double runningProb = 0;
                for (int j = i; j < length; j++) {
                    runningRange += (ranges[i] - prevRange);
                    runningProb += pdf[i];
                    prevRange = ranges[i];
                    if (runningRange <= STOP_RANGE && runningProb >= STOP_PROB) {
                        int speed = (int) startRange;
                        long end = System.nanoTime();
                        if (VERBOSE) {
                            LOGGER.error("Finding max speed prob sampling {} takes {} ms in {} evaluations", speed,
                                    TimeUnit.NANOSECONDS.toMillis(end - begin), evaluations);
                        }
                        return speed;
                    } else if (runningRange > STOP_RANGE) {
                        break;
                    }
                }
            }
        }
    }

    public boolean simulate(double speed, boolean stopOnStalls) {
        scheduler.prepare(speed, initialMemoryComponentCapacities, initialCurrentMemoryComponentCapacity,
                initialFlushedCapacity, initialFlushFinalizedPages, initialFlushSubOperationElapsedTime,
                initialComponentsCapacities, initialMergedCapacities, initialMergeFinalizedPages,
                initialMergeSubOperationElapsedTimes);
        return scheduler.simulate(speed, MAX_TIME, null, stopOnStalls);
    }

    public void print() {
        Component.print();
    }

    private Triple<Boolean, Boolean, Integer> sample(double speed, double stallProb, double sampleConfidence) {
        int ones = 0;
        int n = 1;
        boolean confident = false;
        for (;; n++) {
            boolean stalled = simulate(speed, true);
            if (stalled) {
                ones++;
            }
            double leftSide = CombinatoricsUtils.binomialCoefficientDouble(n, ones) * Math.pow(stallProb, ones)
                    * Math.pow(1 - stallProb, n - ones);
            double rightSide = sampleConfidence / (n + 1);

            if (leftSide <= rightSide || n > MAX_SAMPLE_SIZE) {
                confident = (leftSide <= rightSide);
                break;
            }
        }
        boolean stalled = ones - stallProb * n > 0;
        if (VERBOSE) {
            LOGGER.error("Finding speed {} is {}, samples {}, confident: {} ", speed,
                    stalled ? "stalled" : "not stalled", n, confident);
        }
        return Triple.of(stalled, confident, n);
    }

    private double computeK(double n, double gamma) {
        double eq = n / 2 * (Math.log(n + 1) - Math.log(2) - Math.log(gamma));
        return Math.sqrt(eq);
    }

    private boolean sanityCheck(double[] pdf, int length) {
        double total = 0;
        for (int i = 0; i < length; i++) {
            total += pdf[i];
        }
        assert DoubleUtil.equals(total, 1, 0.00001);
        return true;
    }

    private void computeNext(double[] ranges, double[] pdf, double[] nextRanges, double[] nextPdf, double x, int length,
            double rate) {
        int i = 0;
        double totalPdf = 0;
        for (; i < length; i++) {
            if (ranges[i] < x) {
                nextRanges[i] = ranges[i];
                nextPdf[i] = pdf[i] * 2 * rate;
                totalPdf += nextPdf[i];
            } else {
                break;
            }
        }
        // we will split at p
        nextRanges[i] = x;
        double lastRange = i > 0 ? ranges[i - 1] : 0;
        double diffPdf = (x - lastRange) / (ranges[i] - lastRange) * pdf[i];
        nextPdf[i] = diffPdf * 2 * rate;
        totalPdf += nextPdf[i];
        nextRanges[i + 1] = ranges[i];
        nextPdf[i + 1] = (pdf[i] - diffPdf) * 2 * (1 - rate);
        totalPdf += nextPdf[i + 1];
        i++;
        for (; i < length; i++) {
            nextRanges[i + 1] = ranges[i];
            nextPdf[i + 1] = pdf[i] * 2 * (1 - rate);
            totalPdf += nextPdf[i + 1];
        }

        for (i = 0; i < length + 1; i++) {
            nextPdf[i] = nextPdf[i] / totalPdf;
        }

    }

    private double getNextProbeSpeed(double[] ranges, double[] pdf, int length) {
        double totalProb = 0;
        int i = 0;
        for (; i < length; i++) {
            if (totalProb + pdf[i] > 0.5) {
                break;
            } else {
                totalProb += pdf[i];
            }
        }
        double startRange = i > 0 ? ranges[i - 1] : 0;
        double diffProb = 0.5 - totalProb;
        return diffProb / pdf[i] * (ranges[i] - startRange) + startRange;
    }

    public static class SimulateTerminateException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public final int level;

        public SimulateTerminateException(int level) {
            this.level = level;
        }
    }

}
