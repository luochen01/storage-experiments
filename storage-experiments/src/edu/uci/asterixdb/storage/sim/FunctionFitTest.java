package edu.uci.asterixdb.storage.sim;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.util.Pair;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;

class FunctionFitTest {

    public static void main(String[] args) {
        double[] x = new double[] { 1, 5, 10, 50, 100, 500, 1000, 2000, 3000 };
        double[] y = new double[] { 5, 4.5, 4, 3.5, 3, 2.5, 2, 2, 2 };
        LinearFunction func = new LinearFunction();
        for (int i = 0; i < x.length; i++) {
            func.add(x[i], y[i]);
        }

        XYChart chart = QuickChart.getChart("Sample Chart", "X", "Y", "y(x)", x, y);

        Pair<double[], double[]> pair = sample(1000, 5000, func);
        chart.addSeries("Model", pair.getFirst(), pair.getSecond());
        new SwingWrapper(chart).displayChart();
    }

    private static double a1 = 1;
    private static double b1 = 1;
    private static double c1 = -1;

    private static double func1(double x) {
        return a1 * (Math.pow(Math.E, -b1 * (x + c1)));
    }

    private static double a2 = 2;
    private static double b2 = 0.5;
    private static double c2 = 0.5;

    private static double func2(double x) {
        return a2 * (1 - Math.pow(Math.E, -b2 * (x + c2)));
    }

    private static Pair<double[], double[]> sample(int steps, double max, UnivariateFunction func) {
        double[] x = new double[steps - 1];
        double[] y = new double[steps - 1];
        for (int i = 1; i < steps; i++) {
            x[i - 1] = (double) i / steps * max;
            y[i - 1] = func.value(x[i - 1]);
        }
        return Pair.create(x, y);
    }

    private static double[] sample(double[] x, UnivariateFunction func) {
        double[] y = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            y[i] = func.value(x[i]);
        }
        return y;
    }

}
