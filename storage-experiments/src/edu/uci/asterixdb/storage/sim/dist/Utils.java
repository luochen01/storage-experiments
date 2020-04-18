package edu.uci.asterixdb.storage.sim.dist;

class Utils {

    public static int minIndex(int[] list) {
        int min = -1;
        for (int i = 0; i < list.length; i++) {
            if (min == -1 || list[i] < list[min]) {
                min = i;
            }
        }
        return min;
    }

    public static int minValue(int[] list) {
        return list[minIndex(list)];
    }

    public static int maxIndex(int[] list) {
        int max = -1;
        for (int i = 0; i < list.length; i++) {
            if (max == -1 || list[i] > list[max]) {
                max = i;
            }
        }
        return max;
    }

    public static int maxValue(int[] list) {
        return list[maxIndex(list)];
    }

    public static int percentiple(int low, int high, double percentiple) {
        int range = high - low + 1;
        if (low >= high) {
            range += DistributionSimulator.MAX_KEY;
        }
        return (int) (low + range * percentiple) % DistributionSimulator.MAX_KEY;
    }
}