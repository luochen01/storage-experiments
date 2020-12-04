package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

@FunctionalInterface
interface PolicyCreator {
    DistributionPolicy[] create(int initNodes, int maxNodes, int keysPerNode, int[] partitionsPerNodes);
}

class DistributionSimulator {

    // 100M
    public static final int MAX_KEY = 1000 * 1000 * 1000;

    public static final int NUM_KEYS = 10 * 1000 * 1000;

    public static final int PARTITIONS_PER_NODE = 2;

    public static final int ITERATIONS = 50;

    private static final Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) {
        //        partitionSize(DistributionSimulator::createConsistentPolicies);
        //        partitionSize(DistributionSimulator::createStaticRangePolicies);
        //        partitionSize(DistributionSimulator::createDynamicRangePolicies);
        System.out.println("add nodes");
        addNodes();
        System.out.println("delete nodes");
        deleteNodes();
    }

    private static void deleteNodes() {
        int[] nodes = { 20, 40, 60, 80, 100 };
        int keysPerNode = NUM_KEYS / 100;

        IntList keys = loadKeys(NUM_KEYS);
        // load data

        for (int node : nodes) {
            DistributionPolicy[] policies = createPolicies(node, keysPerNode);
            if (node == nodes[0]) {
                printHeader(policies);
            }
            loadPolicies(policies, keys, 0, node * keysPerNode);

            List<AggregateRebalanceResult> list = new ArrayList<>();
            for (int i = 0; i < policies.length; i++) {
                list.add(new AggregateRebalanceResult());
            }
            for (int i = 0; i < ITERATIONS; i++) {
                for (int p = 0; p < policies.length; p++) {
                    DistributionPolicy policy = policies[p];
                    List<Node> removed =
                            Collections.singletonList(policy.getNodes().get(random.nextInt(policy.getNodes().size())));
                    RebalanceResult result = policy.removeNodes(removed);
                    policy.addNodes(Collections.singletonList(new Node(node + i + 1)));
                    if (i > ITERATIONS / 2) {
                        list.get(p).add(result);
                    }
                }
            }
            printLine(node, (double) keysPerNode * node / (node - 1), list);
        }
    }

    private static void addNodes() {
        int[] nodes = { 20, 40, 60, 80, 100 };
        int keysPerNode = NUM_KEYS / 100;

        IntList keys = loadKeys(NUM_KEYS);
        // load data

        for (int node : nodes) {
            DistributionPolicy[] policies = createPolicies(node, keysPerNode);
            if (node == nodes[0]) {
                printHeader(policies);
            }
            loadPolicies(policies, keys, 0, node * keysPerNode);

            List<AggregateRebalanceResult> list = new ArrayList<>();
            for (int i = 0; i < policies.length; i++) {
                list.add(new AggregateRebalanceResult());
            }
            for (int i = 0; i < ITERATIONS; i++) {
                for (int p = 0; p < policies.length; p++) {
                    DistributionPolicy policy = policies[p];
                    RebalanceResult result = policy.addNodes(Collections.singletonList(new Node(node + i + 1)));
                    List<Node> removed =
                            Collections.singletonList(policy.getNodes().get(random.nextInt(policy.getNodes().size())));
                    policy.removeNodes(removed);
                    if (i > ITERATIONS / 2) {
                        list.get(p).add(result);
                    }
                }
            }
            printLine(node, (double) keysPerNode * node / (node + 1), list);
        }
    }

    private static DistributionPolicy[] createDynamicRangePolicies(int initNodes, int maxNodes, int keysPerNode,
            int[] partitionsPerNodes) {
        DistributionPolicy[] policies = new DistributionPolicy[partitionsPerNodes.length];
        int pos = 0;
        for (int partition : partitionsPerNodes) {
            policies[pos++] = new DynamicPartitionPolicy(keysPerNode / partition, createNodes(initNodes));
        }
        return policies;
    }

    private static DistributionPolicy[] createConsistentPolicies(int initNodes, int maxNodes, int keysPerNode,
            int[] partitionsPerNodes) {
        DistributionPolicy[] policies = new DistributionPolicy[partitionsPerNodes.length];
        int pos = 0;
        for (int partition : partitionsPerNodes) {
            policies[pos++] = new ConsistentHashPolicy(partition, createNodes(initNodes));
        }
        return policies;
    }

    private static DistributionPolicy[] createStaticRangePolicies(int initNodes, int maxNodes, int keysPerNode,
            int[] partitionsPerNodes) {
        DistributionPolicy[] policies = new DistributionPolicy[partitionsPerNodes.length];
        int pos = 0;
        for (int partition : partitionsPerNodes) {
            policies[pos++] = new StaticPartitionPolicy(maxNodes * partition, createNodes(initNodes));
        }
        return policies;
    }

    private static DistributionPolicy[] createPolicies(int nodes, int keysPerNode) {
        return new DistributionPolicy[] { new ConsistentHashPolicy(4, createNodes(nodes)),
                new ConsistentHashPolicy(8, createNodes(nodes)), new ConsistentHashPolicy(16, createNodes(nodes)),
                new StaticPartitionPolicy(4, createNodes(nodes)), new StaticPartitionPolicy(8, createNodes(nodes)),
                new StaticPartitionPolicy(16, createNodes(nodes)) };
    }

    private static IntList loadKeys(int numKeys) {
        IntSet set = new IntOpenHashSet(numKeys);

        while (set.size() < numKeys) {
            set.add(random.nextInt(MAX_KEY));
        }
        IntList list = new IntArrayList(set);
        IntLists.shuffle(list, random);
        return list;
    }

    private static void loadPolicies(DistributionPolicy[] policies) {
        IntList keys = loadKeys(NUM_KEYS);
        loadPolicies(policies, keys, 0, keys.size());
    }

    private static void loadPolicies(DistributionPolicy[] policies, IntList keys, int from, int num) {
        for (DistributionPolicy policy : policies) {
            for (int i = from; i < from + num; i++) {
                policy.load(keys.getInt(i));
            }
        }
    }

    private static void printHeader(DistributionPolicy[] policies) {
        StringBuilder sb = new StringBuilder();
        sb.append("nodes\t");
        for (DistributionPolicy policy : policies) {
            sb.append(policy + "\t");
            sb.append(policy + "\t");
        }
        System.out.println(sb.toString());
    }

    private static void printLine(int nodes, double keysPerNode, List<AggregateRebalanceResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodes);
        sb.append("\t");
        for (AggregateRebalanceResult result : results) {
            sb.append(String.format("%.3f\t%.3f\t", result.movement / result.count / keysPerNode,
                    result.maxKeys / result.count / keysPerNode));
        }
        System.out.println(sb.toString());
    }

    private static List<Node> createNodes(int nodes) {
        List<Node> list = new ArrayList<>();
        for (int i = 1; i <= nodes; i++) {
            list.add(new Node(i));
        }
        return list;
    }

}