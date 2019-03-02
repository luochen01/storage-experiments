package edu.uci.asterixdb.storage.experiments;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Solution {

    static class Pair {
        int i;
        int j;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + i;
            result = prime * result + j;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Pair other = (Pair) obj;
            if (i != other.i)
                return false;
            if (j != other.j)
                return false;
            return true;
        }

        public Pair(int i, int j) {
            this.i = i;
            this.j = j;
        }

    }

    public static int countMatches(List<String> grid1, List<String> grid2) {
        List<Set<Pair>> regions1 = collectRegions(grid1);
        List<Set<Pair>> regions2 = collectRegions(grid2);
        int count = 0;
        for (Set<Pair> region1 : regions1) {
            if (regions2.contains(region1)) {
                count++;
            }
        }
        return count;
    }

    private static List<Set<Pair>> collectRegions(List<String> grid) {
        boolean[][] visited = new boolean[grid.size()][grid.get(0).length()];
        List<Set<Pair>> regions = new ArrayList<>();
        for (int i = 0; i < grid.size(); i++) {
            String str = grid.get(i);
            for (int j = 0; j < str.length(); j++) {
                if (!visited[i][j] && str.charAt(j) == '1') {
                    Set<Pair> region = new HashSet<>();
                    collectRegion(grid, i, j, visited, region);
                    regions.add(region);
                }
            }
        }
        return regions;
    }

    private static void collectRegion(List<String> grid, int i, int j, boolean[][] visited, Set<Pair> region) {
        if (i < 0 || j < 0 || i >= grid.size() || j >= grid.get(i).length() || visited[i][j]) {
            return;
        }
        visited[i][j] = true;
        if (grid.get(i).charAt(j) == '1') {
            region.add(new Pair(i, j));
            collectRegion(grid, i + 1, j, visited, region);
            collectRegion(grid, i - 1, j, visited, region);
            collectRegion(grid, i, j + 1, visited, region);
            collectRegion(grid, i, j - 1, visited, region);
        }
    }

}
