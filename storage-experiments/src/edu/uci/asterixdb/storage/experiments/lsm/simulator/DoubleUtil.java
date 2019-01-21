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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;

public class DoubleUtil {

    private static final double epsilon = 0.000001;

    public static boolean equals(double a, double b) {
        return equals(a, b, epsilon);
    }

    public static boolean equals(double a, double b, double precision) {
        return Math.abs(a - b) < precision;
    }

    public static boolean greaterThan(double a, double b) {
        return a - b > epsilon;
    }

    public static boolean greaterThanOrEqualTo(double a, double b) {
        double diff = a - b;
        return diff > epsilon || Math.abs(diff) < epsilon;
    }

    public static boolean lessThanOrEqualTo(double a, double b) {
        double diff = a - b;
        return diff < -epsilon || Math.abs(diff) < epsilon;
    }

    public static double sum(double[] values) {
        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            sum += values[i];
        }
        return sum;
    }

    private static final NumberFormat formatter = new DecimalFormat("#0.000");
    private static final NumberFormat timeFormatter = new DecimalFormat("#0");

    public static void addIndent(int indent, StringBuilder sb) {
        for (int i = 0; i < indent; i++) {
            sb.append('-');
        }
    }

    public static String formatDouble(double value) {
        return formatter.format(value);
    }

    public static String toShortString(RandomVariable v) {
        if (v == null) {
            return null;
        } else {
            return formatDouble(v.mean);
        }
    }

    public static String formatTime(double time) {
        return timeFormatter.format(time);
    }

    public static int getMultiplier(double big, double small) {
        return (int) Math.round(Math.max(1, big / small));
    }

    private static final Random RAND = new Random(17);

    private static final double[] Gaussians = new double[4096];

    static {
        for (int i = 0; i < Gaussians.length; i++) {
            Gaussians[i] = RAND.nextGaussian();
        }
    }

    public static double nextGaussian(RandomVariable v) {
        assert v != null;
        assert v.min <= v.mean;
        assert v.max >= v.mean;
        double value = Gaussians[RAND.nextInt(Gaussians.length)] * v.std + v.mean;
        if (value > v.max) {
            return v.max;
        } else if (value < v.min) {
            return v.min;
        } else {
            return value;
        }
    }

}
