package edu.uci.asterixdb.storage.sim.dist;

import java.util.Collections;
import java.util.List;

class Utils {

    public static int getTotalKeys(List<Node> nodes) {
        int totalKeys = 0;
        for (Node node : nodes) {
            totalKeys += node.assignment.getTotalKeys();
        }
        return totalKeys;
    }

    public static Node minPartitionsNode(List<Node> nodes) {
        return Collections.min(nodes, (n1, n2) -> Integer.compare(n1.numPartitions(), n2.numPartitions()));
    }

    public static Node maxPartitionsNode(List<Node> nodes) {
        return Collections.min(nodes, (n1, n2) -> -Integer.compare(n1.numPartitions(), n2.numPartitions()));
    }

    public static Node minKeysNode(List<Node> nodes) {
        return Collections.min(nodes, (n1, n2) -> Integer.compare(n1.numKeys(), n2.numKeys()));
    }

    public static Node maxKeysNode(List<Node> nodes) {
        return Collections.min(nodes, (n1, n2) -> -Integer.compare(n1.numKeys(), n2.numKeys()));
    }

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