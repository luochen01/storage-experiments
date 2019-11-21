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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.analysis.DifferentiableUnivariateFunction;
import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.exception.NoDataException;

public class CombinedFunction implements DifferentiableUnivariateFunction, Serializable {

    private static final long serialVersionUID = 1L;

    private final IncreaseFunction inc;
    private final DecreaseFunction dec;

    private final UnivariateFunction incDerivative;
    private final UnivariateFunction decDerivative;

    public CombinedFunction(double a, double b, double c, double d) {
        super();
        this.inc = new IncreaseFunction(a, b);
        this.dec = new DecreaseFunction(c, d);

        this.incDerivative = inc.derivative();
        this.decDerivative = dec.derivative();
    }

    @Override
    public UnivariateFunction derivative() {
        return new UnivariateFunction() {

            @Override
            public double value(double x) {
                return incDerivative.value(x) + decDerivative.value(x);
            }
        };
    }

    @Override
    public double value(double x) {
        return inc.value(x) + dec.value(x);
    }

    @Override
    public String toString() {
        return String.format("%s + %s", inc, dec);
    }

    /**
     * Dedicated parametric polynomial class.
     *
     * @since 3.0
     */
    public static class Parametric implements ParametricUnivariateFunction {
        private final IncreaseFunction.Parametric incParametric;
        private final DecreaseFunction.Parametric decParametric;

        public Parametric() {
            this.incParametric = new IncreaseFunction.Parametric();
            this.decParametric = new DecreaseFunction.Parametric();
        }

        /** {@inheritDoc} */
        @Override
        public double[] gradient(double x, double... parameters) {
            double[] inc = incParametric.gradient(x, parameters[0], parameters[1]);
            double[] dec = decParametric.gradient(x, parameters[2], parameters[3]);
            return ArrayUtils.addAll(inc, dec);
        }

        /** {@inheritDoc} */
        @Override
        public double value(final double x, final double... parameters) throws NoDataException {
            return IncreaseFunction.evaluate(x, parameters[0], parameters[1])
                    + DecreaseFunction.evaluate(x, parameters[2], parameters[3]);
        }
    }
}
