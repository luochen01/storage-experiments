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

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.Pair;

public class LatencyArray {

    private final double unit;

    private final SortedMap<Integer, Long> map = new TreeMap<>();

    private long totalCount = 0;

    private double totalLatency = 0;

    public LatencyArray(int precision) {
        assert precision > 0;
        this.unit = 1.0 / precision;
    }

    private int getMultiplier(double latency) {
        return (int) Math.ceil(latency / unit);
    }

    public void add(double latency, int count) {
        int multiplier = getMultiplier(latency);
        Long existing = map.getOrDefault(multiplier, 0l);
        map.put(multiplier, existing + count);
        totalCount += count;
        totalLatency += count * latency;
    }

    public double getPrecision() {
        return unit;
    }

    public double getTotalLatency() {
        return totalLatency;
    }

    public void forEach(BiFunction<Double, Long, Void> func) {
        for (Entry<Integer, Long> e : map.entrySet()) {
            func.apply(e.getKey() * unit, e.getValue());
        }
    }

    public Pair<double[], long[]> toArrays() {
        double[] latencies = new double[map.size()];
        long[] counts = new long[map.size()];
        int i = 0;
        for (Entry<Integer, Long> e : map.entrySet()) {
            latencies[i] = e.getKey() * unit;
            counts[i] = e.getValue();
            i++;
        }
        return Pair.of(latencies, counts);
    }

    public int getDistinctKeys() {
        return map.size();
    }

    public long getTotalCount() {
        return totalCount;
    }

    public double getMaxLatency() {
        return (double) map.lastKey() * unit;
    }
}
