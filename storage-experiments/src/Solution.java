import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class UnionFind {
    final Map<String, String> parents = new HashMap<>();
    final Map<String, Double> factors = new HashMap<>();

    public String find(String node) {
        String parent = parents.get(node);
        if (parent == null) {
            return node;
        } else {
            String root = find(parent);
            parents.put(node, root);
            factors.put(node, factors.getOrDefault(parent, 1.0) * factors.getOrDefault(node, 1.0));
            return root;
        }
    }

    public void union(String node1, String node2, double factor) {
        String root1 = find(node1);
        String root2 = find(node2);

        parents.put(root2, root1);
        factors.put(root2, factor * factors.getOrDefault(node1, 1.0) / factors.getOrDefault(node2, 1.0));
    }
}

class Solution {
    public double[] calcEquation(List<List<String>> equations, double[] values, List<List<String>> queries) {
        UnionFind uf = new UnionFind();
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < values.length; i++) {
            List<String> list = equations.get(i);
            uf.union(list.get(0), list.get(1), values[i]);
            keys.addAll(list);
        }
        double[] result = new double[queries.size()];
        for (int i = 0; i < result.length; i++) {
            List<String> list = queries.get(i);
            if (!keys.contains(list.get(0)) && !keys.contains(list.get(1))) {
                result[i] = -1.0;
            } else {
                String root1 = uf.find(list.get(0));
                String root2 = uf.find(list.get(1));
                if (root1.equals(root2)) {
                    result[i] = uf.factors.getOrDefault(list.get(1), 1.0) / uf.factors.getOrDefault(list.get(0), 1.0);
                } else {
                    result[i] = -1.0;
                }
            }
        }
        return result;
    }

    public static void main(String[] args) {
        new Solution().calcEquation(
                Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("b", "c"), Arrays.asList("d", "c")),
                new double[] { 2.0, 3.0, 5.0 }, Arrays.asList(Arrays.asList("a", "d")));
    }

}