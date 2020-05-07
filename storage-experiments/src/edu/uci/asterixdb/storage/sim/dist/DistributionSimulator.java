package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

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
    public static final int MAX_KEY = 100 * 1024 * 1024;

    public static final int NUM_KEYS = 1000 * 1000;

    public static final int PARTITIONS_PER_NODE = 16;

    private static final Random random = new Random(17);

    public static void main(String[] args) {
        //        partitionSize(DistributionSimulator::createConsistentPolicies);
        //        partitionSize(DistributionSimulator::createStaticRangePolicies);
        //        partitionSize(DistributionSimulator::createDynamicRangePolicies);

        //addNodes();
        deleteNodes();
    }

    private static void addNodes() {
        int initNodes = 2;
        int maxNodes = 100;
        int keysPerNode = NUM_KEYS / maxNodes;
        DistributionPolicy[] policies = createPolicies(initNodes, maxNodes, keysPerNode);

        IntList keys = loadKeys();
        loadPolicies(policies, keys, 0, initNodes * keysPerNode);
        // load data
        printHeader(policies);

        for (int id = initNodes + 1; id <= maxNodes; id++) {
            loadPolicies(policies, keys, (id - 1) * keysPerNode, keysPerNode);
            List<RebalanceResult> list = new ArrayList<>();
            for (DistributionPolicy policy : policies) {
                list.add(policy.addNodes(Collections.singletonList(new Node(id))));
            }
            printLine(id, keysPerNode, list);
        }
    }

    private static void partitionSize(PolicyCreator creator) {
        int initNodes = 2;
        int maxNodes = 100;
        int keysPerNode = NUM_KEYS / maxNodes;

        DistributionPolicy[] policies = creator.create(initNodes, maxNodes, keysPerNode, new int[] { 2, 4, 8, 16, 32 });

        IntList keys = loadKeys();
        loadPolicies(policies, keys, 0, initNodes * keysPerNode);
        // load data
        printHeader(policies);

        for (int id = initNodes + 1; id <= maxNodes; id++) {
            loadPolicies(policies, keys, (id - 1) * keysPerNode, keysPerNode);
            List<RebalanceResult> list = new ArrayList<>();
            for (DistributionPolicy policy : policies) {
                list.add(policy.addNodes(Collections.singletonList(new Node(id))));
            }
            printLine(id, keysPerNode, list);
        }
    }

    private static void deleteNodes() {
        int initNodes = 100;
        int keysPerNode = NUM_KEYS / initNodes;
        DistributionPolicy[] policies = createPolicies(initNodes, initNodes, keysPerNode);
        loadPolicies(policies);
        // load data
        printHeader(policies);

        for (int i = 0; i < 100; i++) {
            int node = ThreadLocalRandom.current().nextInt(initNodes);
            List<RebalanceResult> list = new ArrayList<>();
            for (DistributionPolicy policy : policies) {
                RebalanceResult result = policy.removeNodes(Collections.singletonList(policy.getNodes().get(node)));
                policy.addNodes(Collections.singletonList(new Node(initNodes + i + 1)));

                list.add(result);

            }
            printLine(initNodes - 1, keysPerNode, list);
        }
    }

    private static DistributionPolicy[] createDynamicRangePolicies(int initNodes, int maxNodes, int keysPerNode,
            int[] partitionsPerNodes) {
        DistributionPolicy[] policies = new DistributionPolicy[partitionsPerNodes.length];
        int pos = 0;
        for (int partition : partitionsPerNodes) {
            policies[pos++] = new DynamicRangePolicy(keysPerNode / partition, createNodes(initNodes));
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

    private static DistributionPolicy[] createPolicies(int initNodes, int maxNodes, int keysPerNode) {
        return new DistributionPolicy[] { new HashPolicy(createNodes(initNodes)),
                new LinearHashPolicy(createNodes(initNodes)), new RangePolicy(createNodes(initNodes)),
                new ConsistentHashPolicy(PARTITIONS_PER_NODE, createNodes(initNodes)),
                new StaticPartitionPolicy(maxNodes * PARTITIONS_PER_NODE, createNodes(initNodes)),
                new DynamicRangePolicy(keysPerNode / PARTITIONS_PER_NODE, createNodes(initNodes)) };
    }

    private static IntList loadKeys() {
        IntSet set = new IntOpenHashSet(NUM_KEYS);

        while (set.size() < NUM_KEYS) {
            set.add(random.nextInt(MAX_KEY));
        }
        IntList list = new IntArrayList(set);
        IntLists.shuffle(list, random);
        return list;
    }

    private static void loadPolicies(DistributionPolicy[] policies) {
        IntList keys = loadKeys();
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

    private static void printLine(int nodes, int keysPerNode, List<RebalanceResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodes);
        sb.append("\t");
        for (RebalanceResult result : results) {
            sb.append(String.format("%.3f\t%.3f\t", (double) result.movement / keysPerNode,
                    (double) result.maxKeys / keysPerNode));
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