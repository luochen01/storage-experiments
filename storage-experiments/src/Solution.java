class Solution {
    private final int MAX = 100000000;

    public int minCost(int[] houses, int[][] cost, int m, int n, int target) {
        int[][][] dp = new int[m + 1][n + 1][target + 1];
        for (int i = 0; i <= m; i++) {
            for (int j = 0; j <= n; j++) {
                for (int k = 0; k <= target; k++) {
                    dp[i][j][k] = MAX;
                }
            }
        }
        for (int j = 1; j <= n; j++) {
            dp[0][j][0] = 0;
            dp[0][j][1] = 0;
        }
        for (int k = 1; k <= target; k++) {
            for (int i = 1; i <= m; i++) {
                for (int j = 1; j <= n; j++) {
                    if (houses[i - 1] == 0) {
                        for (int j2 = 1; j2 <= n; j2++) {
                            if (j != j2) {
                                dp[i][j][k] = Math.min(dp[i][j][k], dp[i - 1][j2][k - 1] + cost[i - 1][j - 1]);
                            } else {
                                dp[i][j][k] = Math.min(dp[i][j][k], dp[i - 1][j2][k] + cost[i - 1][j - 1]);
                            }
                        }
                    } else if (houses[i - 1] == j) {
                        for (int j2 = 1; j2 <= n; j2++) {
                            if (j != j2) {
                                dp[i][j][k] = Math.min(dp[i][j][k], dp[i - 1][j2][k - 1]);
                            } else {
                                dp[i][j][k] = Math.min(dp[i][j][k], dp[i - 1][j][k]);
                            }
                        }
                    }
                }
            }
        }

        int min = MAX;
        for (int j = 1; j <= n; j++) {
            min = Math.min(min, dp[m][j][target]);
        }
        return min < MAX ? min : -1;
    }

    public static void main(String[] args) {
        new Solution().minCost(new int[] { 0, 2, 1, 2, 0 },
                new int[][] { { 1, 10 }, { 10, 1 }, { 10, 1 }, { 1, 10 }, { 5, 1 } }, 5, 2, 3);
    }
}