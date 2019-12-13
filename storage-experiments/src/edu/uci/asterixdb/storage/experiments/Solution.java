package edu.uci.asterixdb.storage.experiments;

public class Solution {
    public int mergeStones(int[] stones, int K) {
        if ((stones.length - 1) % (K - 1) != 0) {
            return -1;
        }
        int[] sums = new int[stones.length];
        sums[0] = stones[0];
        for (int i = 1; i < stones.length; i++) {
            sums[i] = sums[i - 1] + stones[i];
        }
        int[][] dp = new int[stones.length][stones.length];
        return solve(dp, sums, K, 0, dp.length - 1);
    }

    private int solve(int[][] dp, int[] sums, int K, int from, int to) {
        int len = (to - from + 1);
        if (len <= 1) {
            return 0;
        }
        if ((len - 1) % (K - 1) != 0) {
            return -1;
        }

        if (dp[from][to] != 0) {
            return dp[from][to];
        }

        int min = Integer.MAX_VALUE;

        int[] splits = new int[K - 1];
        for (int i = 0; i < splits.length; i++) {
            splits[i] = from + i + 1;
        }
        while (true) {
            int cost = compute(dp, sums, K, from, to, splits);
            if (cost >= 0) {
                min = Math.min(cost, min);
            }

            if (!increment(splits, from, to, K)) {
                break;
            }
        }

        dp[from][to] = min + getSum(sums, from, to);
        return dp[from][to];
    }

    private int compute(int[][] dp, int[] sums, int K, int from, int to, int[] splits) {
        int sum = solve(dp, sums, K, from, splits[0] - 1);
        for (int i = 0; i < splits.length - 1; i++) {
            int begin = splits[i];
            int end = splits[i + 1] - 1;
            sum += solve(dp, sums, K, begin, end);
        }
        sum += solve(dp, sums, K, splits[splits.length - 1], to);
        return sum;
    }

    private int getSum(int[] sums, int i, int j) {
        if (i == 0) {
            return sums[j];
        } else {
            return sums[j] - sums[i - 1];
        }
    }

    private boolean increment(int[] splits, int from, int to, int K) {
        for (int i = splits.length - 1; i >= 0; i--) {
            if (splits[i] + (splits.length - i - 1) < to) {
                splits[i] += (K - 1);
                for (int j = i + 1; j < splits.length; j++) {
                    splits[j] = splits[j - 1] + 1;
                }
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {

        System.out.println(
                new Solution().mergeStones(new int[] { 36, 2, 61, 30, 74, 35, 65, 31, 43, 92, 15, 11, 22 }, 5));
    }
}