/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.asterixdb.storage.sim;

import java.util.Arrays;

import org.apache.commons.math3.analysis.UnivariateFunction;

import it.unimi.dsi.fastutil.doubles.Double2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleSortedMap;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;

public class LinearFunction implements UnivariateFunction {

    private static final long serialVersionUID = 1L;

    private final Double2DoubleSortedMap points = new Double2DoubleAVLTreeMap(new DoubleComparator() {
        @Override
        public int compare(double k1, double k2) {
            if (Math.abs(k1 - k2) < 0.1) {
                return 0;
            } else {
                return Double.compare(k1, k2);
            }
        }
    });

    private UnivariateFunction function = null;

    public LinearFunction() {
    }

    public void add(double x, double y) {
        points.put(x, y);
    }

    @Override
    public double value(double x) {
        if (function == null) {
            // initialize function
            double[] xs = new double[points.size()];
            double[] ys = new double[points.size()];
            int i = 0;

            for (Double2DoubleMap.Entry e : points.double2DoubleEntrySet()) {
                xs[i] = e.getDoubleKey();
                ys[i] = e.getDoubleValue();
                i++;
            }
            int lines = points.size() - 1;
            // Slope of the lines between the datapoints.
            final double[] slopes = new double[lines];
            final double[] intercepts = new double[lines];
            for (i = 0; i < lines; i++) {
                slopes[i] = (ys[i + 1] - ys[i]) / (xs[i + 1] - xs[i]);
                intercepts[i] = (xs[i + 1] * ys[i] - xs[i] * ys[i + 1]) / (xs[i + 1] - xs[i]);
            }

            function = new LinearInterpolateFunction(xs, slopes, intercepts);
        }
        return function.value(x);
    }

    private static class LinearInterpolateFunction implements UnivariateFunction {
        private final double[] xs;
        private final double[] slopes;
        private final double[] intercepts;

        public LinearInterpolateFunction(double[] xs, double[] slopes, double[] intercepts) {
            this.xs = xs;
            this.slopes = slopes;
            this.intercepts = intercepts;
        }

        @Override
        public double value(double x) {
            int index = Arrays.binarySearch(xs, x);
            if (index < 0) {
                index = -index - 2;
            }
            index = Math.min(index, slopes.length - 1);
            return slopes[index] * x + intercepts[index];
        }
    }

}
