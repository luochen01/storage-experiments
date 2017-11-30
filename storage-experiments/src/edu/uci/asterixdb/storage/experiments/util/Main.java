package edu.uci.asterixdb.storage.experiments.util;

public class Main {

    public static void main(String[] args) {
        System.out.println(time(10, 300));
        System.out.println(time(10, 700));
        System.out.println(time(10, 2000));

        System.out.println(time(100, 300));
        System.out.println(time(100, 700));
        System.out.println(time(100, 2000));

        System.out.println(time(1000, 300));
        System.out.println(time(1000, 700));
        System.out.println(time(1000, 2000));
    }

    public static double time(double N, double u) {
        double rate = Math.min(30 * 1000 / N + u, 2 * 1000);
        return 15 * 1000000 / rate;
    }

}
