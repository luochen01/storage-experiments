package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

class DistributionSimulator {
    public static final int MAX_KEY = 1 * 1024 * 1024;

    public static final int PARTITIONS_PER_NODE = 16;

    public static void main(String[] args) {
        //addNodes();
        deleteNodes();
    }

    private static void addNodes() {
        int initNodes = 2;
        int maxNodes = 100;
        DistributionPolicy[] policies = createPolicies(initNodes, maxNodes);
        loadPolicies(policies);
        // load data
        printHeader(policies);

        for (int id = initNodes + 1; id <= maxNodes; id++) {
            List<RebalanceResult> list = new ArrayList<>();
            for (DistributionPolicy policy : policies) {
                list.add(policy.addNodes(Collections.singletonList(new Node(id))));
            }
            printLine(id, list);
        }
    }

    private static void deleteNodes() {
        int initNodes = 100;
        DistributionPolicy[] policies = createPolicies(initNodes, initNodes);
        loadPolicies(policies);
        // load data
        printHeader(policies);

        for (int nodes = initNodes - 1; nodes >= 2; nodes--) {
            int node = ThreadLocalRandom.current().nextInt(nodes);

            List<RebalanceResult> list = new ArrayList<>();
            for (DistributionPolicy policy : policies) {
                RebalanceResult result = policy.removeNodes(Collections.singletonList(policy.getNodes().get(node)));
                list.add(result);
            }
            printLine(nodes, list);
        }
    }

    private static DistributionPolicy[] createPolicies(int initNodes, int maxNodes) {
        return new DistributionPolicy[] { new HashPolicy(createNodes(initNodes)),
                new LinearHashPolicy(createNodes(initNodes)), new RangePolicy(createNodes(initNodes)),
                new ConsistentHashPolicy(PARTITIONS_PER_NODE, createNodes(initNodes)),
                new StaticPartitionPolicy(maxNodes * PARTITIONS_PER_NODE, createNodes(initNodes)),
                new DynamicRangePolicy(MAX_KEY / PARTITIONS_PER_NODE / maxNodes, createNodes(initNodes)) };
    }

    private static void loadPolicies(DistributionPolicy[] policies) {
        IntList keys = new IntArrayList();
        for (int i = 0; i < MAX_KEY; i++) {
            keys.add(i);
        }
        IntLists.shuffle(keys, ThreadLocalRandom.current());
        for (DistributionPolicy policy : policies) {
            for (int key : keys) {
                policy.load(key);
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

    private static void printLine(int nodes, List<RebalanceResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodes);
        sb.append("\t");
        for (RebalanceResult result : results) {
            sb.append(String.format("%.3f\t%.3f\t", (double) result.movement / MAX_KEY,
                    result.maxKeys / ((double) MAX_KEY / nodes)));
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