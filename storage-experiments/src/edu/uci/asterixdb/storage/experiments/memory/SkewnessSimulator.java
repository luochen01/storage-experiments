package edu.uci.asterixdb.storage.experiments.memory;

public class SkewnessSimulator {

    public static void main(String[] args) {
        double[] skews = { 0.2, 0.4, 0.6, 0.8 };

        for (double skew : skews) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 2; i++) {
                sb.append(String.format("%.3f,", skew / 2));
            }
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%.3f,", (1 - skew) / 8));
            }
            sb.deleteCharAt(sb.length() - 1);
            System.out.println(sb.toString());
        }

    }

}
