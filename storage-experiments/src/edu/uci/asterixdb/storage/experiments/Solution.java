package edu.uci.asterixdb.storage.experiments;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

class Solution {
    public int[][] kClosest(int[][] points, int K) {
        if (points.length <= K) {
            return points;
        }

        sort(points, 0, points.length - 1, K);
        return Arrays.copyOfRange(points, 0, K);
    }

    private void sort(int[][] points, int from, int to, int K) {
        if (from >= to || K == 0) {
            return;
        }

        int mid = partition(points, from, to);
        int len = mid - from + 1;
        if (len < K) {
            sort(points, mid + 1, to, K - len);
        } else if (len > K) {
            sort(points, from, mid - 1, K);
        }
    }

    private int partition(int[][] points, int from, int to) {
        int p = ThreadLocalRandom.current().nextInt(from, to);
        swap(points, from, p);
        int left = from + 1;
        int right = to;
        int pivot = dist(points[from]);
        while (true) {
            while (left < right && dist(points[left]) < pivot) {
                left++;
            }
            while (left <= right && dist(points[right]) > pivot) {
                right--;
            }
            if (left >= right) {
                break;
            }
            swap(points, left, right);
        }
        swap(points, from, right);
        return right;
    }

    private void swap(int[][] points, int i, int j) {
        int[] tmp = points[i];
        points[i] = points[j];
        points[j] = tmp;
    }

    private int dist(int[] p) {
        return p[0] * p[0] + p[1] * p[1];
    }

    public static void main(String[] args) {
        new Solution().kClosest(new int[][] { { 2, 2 }, { 2, 2 }, { 2, 2 }, { 2, 2 }, { 1, 1 } }, 1);
    }

}