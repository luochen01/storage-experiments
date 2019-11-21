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

import java.io.Serializable;

import org.apache.commons.math3.analysis.DifferentiableUnivariateFunction;
import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.exception.NoDataException;

public class WriteAmpFunction implements DifferentiableUnivariateFunction, Serializable {

    private static final long serialVersionUID = 1L;

    private final double a;
    private final double b;

    public WriteAmpFunction(double[] params) {
        this(params[0], params[1]);
    }

    public WriteAmpFunction(double a, double b) {
        super();
        this.a = a;
        this.b = b;
    }

    @Override
    public UnivariateFunction derivative() {
        return new UnivariateFunction() {
            @Override
            public double value(double x) {
                return -b / x;
            }
        };
    }

    @Override
    public double value(double x) {
        return evaluate(x, a, b);
    }

    protected static double evaluate(double x, double a, double b) {
        return a - b * Math.log(x);
    }

    @Override
    public String toString() {
        return String.format("%f - %f * ln(x))", a, b);
    }

    public static class Parametric implements ParametricUnivariateFunction {
        /** {@inheritDoc} */
        @Override
        public double[] gradient(double x, double... parameters) {
            final double[] gradient = new double[2];
            gradient[0] = 1;
            gradient[1] = -Math.log(x);
            return gradient;
        }

        /** {@inheritDoc} */
        @Override
        public double value(final double x, final double... parameters) throws NoDataException {
            return evaluate(x, parameters[0], parameters[1]);
        }
    }
}
