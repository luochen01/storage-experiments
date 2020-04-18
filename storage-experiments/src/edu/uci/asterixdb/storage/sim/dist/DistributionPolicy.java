package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.IntArrayList;

class RebalanceResult {
    final int movement;
    final int minKeys;
    final int maxKeys;

    public RebalanceResult(int movement, int minKeys, int maxKeys) {
        this.movement = movement;
        this.minKeys = minKeys;
        this.maxKeys = maxKeys;

        Preconditions.checkState(minKeys <= maxKeys);
    }
}

interface DistributionPolicy {
    RebalanceResult addNodes(List<Node> nodes);

    RebalanceResult removeNodes(List<Node> nodes);

    void load(int key);

    List<Node> getNodes();
}

class HashPolicy implements DistributionPolicy {
    private List<Node> nodes = new ArrayList<>();

    public HashPolicy(List<Node> nodes) {
        this.nodes.addAll(nodes);
    }

    @Override
    public RebalanceResult addNodes(List<Node> added) {
        List<Node> newNodes = new ArrayList<>(this.nodes);
        newNodes.addAll(added);
        RebalanceResult result = computeMovement(this.nodes, newNodes);
        this.nodes = newNodes;
        return result;
    }

    @Override
    public RebalanceResult removeNodes(List<Node> deleted) {
        List<Node> newNodes = new ArrayList<>(this.nodes);
        newNodes.removeAll(deleted);
        RebalanceResult result = computeMovement(this.nodes, newNodes);
        this.nodes = newNodes;
        return result;
    }

    private RebalanceResult computeMovement(List<Node> oldNodes, List<Node> newNodes) {
        int count = 0;
        int[] keysPerNode = new int[newNodes.size()];
        for (int i = 0; i < DistributionSimulator.MAX_KEY; i++) {
            Node node1 = oldNodes.get(i % oldNodes.size());
            Node node2 = newNodes.get(i % newNodes.size());
            if (node1.id != node2.id) {
                count++;
            }
            keysPerNode[i % newNodes.size()]++;
        }
        return new RebalanceResult(count, Utils.minValue(keysPerNode), Utils.maxValue(keysPerNode));
    }

    @Override
    public void load(int key) {
    }

    @Override
    public String toString() {
        return "hashing";
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }
}

class LinearHashPolicy implements DistributionPolicy {
    private List<Node> nodes = new ArrayList<>();

    private int base;
    private int splitted;

    public LinearHashPolicy(List<Node> nodes) {
        base = 1;
        splitted = 0;
        this.nodes.add(nodes.get(0));
        for (int i = 1; i < nodes.size(); i++) {
            addNode(nodes.get(i));
        }
    }

    private void addNode(Node node) {
        nodes.add(node);
        splitted++;
        if (splitted == base) {
            splitted = 0;
            base *= 2;
        }
        Preconditions.checkState(base + splitted == nodes.size());
    }

    private void decreaseStep() {
        if (splitted == 0) {
            base /= 2;
            splitted = base - 1;
        } else {
            splitted--;
        }
    }

    @Override
    public RebalanceResult addNodes(List<Node> nodes) {
        LinearHashPolicy newPolicy = this.clone();
        for (int i = 0; i < nodes.size(); i++) {
            newPolicy.addNode(nodes.get(i));
        }
        RebalanceResult result = computeMovement(newPolicy);
        copy(newPolicy);
        return result;
    }

    private void copy(LinearHashPolicy newPolicy) {
        this.nodes = newPolicy.nodes;
        this.base = newPolicy.base;
        this.splitted = newPolicy.splitted;
    }

    @Override
    public RebalanceResult removeNodes(List<Node> nodes) {
        LinearHashPolicy newPolicy = this.clone();
        newPolicy.nodes.removeAll(nodes);
        for (int i = 0; i < nodes.size(); i++) {
            newPolicy.decreaseStep();
        }
        RebalanceResult result = computeMovement(newPolicy);
        copy(newPolicy);
        return result;
    }

    private RebalanceResult computeMovement(LinearHashPolicy newPolicy) {
        int[] keysPerNode = new int[newPolicy.nodes.size()];
        int count = 0;
        for (int i = 0; i < DistributionSimulator.MAX_KEY; i++) {
            Node node1 = this.assign(i);
            Node node2 = newPolicy.assign(i);
            if (node1 != node2) {
                count++;
            }
            keysPerNode[newPolicy.assignIndex(i)]++;
        }
        return new RebalanceResult(count, Utils.minValue(keysPerNode), Utils.maxValue(keysPerNode));
    }

    private int assignIndex(int key) {
        int pos = key % (base << 1);
        if (pos >= base + splitted) {
            pos -= base;
        }
        return pos;
    }

    private Node assign(int key) {
        return nodes.get(assignIndex(key));
    }

    @Override
    public LinearHashPolicy clone() {
        LinearHashPolicy policy = new LinearHashPolicy(nodes);
        policy.base = base;
        policy.splitted = splitted;
        return policy;
    }

    @Override
    public void load(int key) {
    }

    @Override
    public String toString() {
        return "linear-hashing";
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}

class RangePolicy implements DistributionPolicy {
    private List<Node> nodes = new ArrayList<>();
    private int[] upperBounds;

    public RangePolicy(List<Node> nodes) {
        this.nodes.addAll(nodes);
        upperBounds = computeUpperBounds(this.nodes);
    }

    @Override
    public RebalanceResult addNodes(List<Node> nodes) {
        List<Node> newNodes = new ArrayList<>();

        int stepSize = this.nodes.size() / (nodes.size() + 1);
        int pos = 0;

        for (int i = 0; i < this.nodes.size(); i++) {
            newNodes.add(this.nodes.get(i));
            if (pos < nodes.size() && (i + 1) % stepSize == 0) {
                newNodes.add(nodes.get(pos++));
            }
        }
        while (pos < nodes.size()) {
            newNodes.add(nodes.get(pos++));
        }

        int[] newUpperBounds = computeUpperBounds(newNodes);
        RebalanceResult result = computeMovement(this.nodes, this.upperBounds, newNodes, newUpperBounds);
        this.nodes = newNodes;
        this.upperBounds = newUpperBounds;
        return result;
    }

    @Override
    public RebalanceResult removeNodes(List<Node> nodes) {
        List<Node> newNodes = new ArrayList<>(this.nodes);
        newNodes.removeAll(nodes);
        int[] newUpperBounds = computeUpperBounds(newNodes);
        RebalanceResult result = computeMovement(this.nodes, this.upperBounds, newNodes, newUpperBounds);
        this.nodes = newNodes;
        this.upperBounds = newUpperBounds;
        return result;
    }

    private RebalanceResult computeMovement(List<Node> oldNodes, int[] oldUpperBounds, List<Node> newNodes,
            int[] newUpperBounds) {
        int pos1 = 0;
        int pos2 = 0;
        int count = 0;
        int[] keysPerNode = new int[newNodes.size()];
        for (int i = 0; i < DistributionSimulator.MAX_KEY; i++) {
            while (i > oldUpperBounds[pos1]) {
                pos1++;
            }
            while (i > newUpperBounds[pos2]) {
                pos2++;
            }
            Node node1 = oldNodes.get(pos1);
            Node node2 = newNodes.get(pos2);
            if (node1.id != node2.id) {
                count++;
            }
            keysPerNode[pos2]++;
        }
        return new RebalanceResult(count, Utils.minValue(keysPerNode), Utils.maxValue(keysPerNode));
    }

    @Override
    public void load(int key) {
    }

    private int[] computeUpperBounds(List<Node> nodes) {
        int[] upperBounds = new int[nodes.size()];
        double step = (double) DistributionSimulator.MAX_KEY / nodes.size();
        for (int i = 0; i < nodes.size(); i++) {
            upperBounds[i] = (int) (step * (i + 1));
        }
        return upperBounds;
    }

    @Override
    public String toString() {
        return "range";
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}

class StaticPartitionPolicy implements DistributionPolicy {
    private final List<Node> nodes = new ArrayList<>();
    private int[] nodePartitions;
    private final Partition[] partitions;

    public StaticPartitionPolicy(int partitions, List<Node> nodes) {
        this.partitions = new Partition[partitions];
        this.nodes.addAll(nodes);
        this.nodePartitions = new int[nodes.size()];
        double step = (double) DistributionSimulator.MAX_KEY / this.partitions.length;
        for (int i = 0; i < partitions; i++) {
            int low = (int) (i * step);
            int high = (int) ((i + 1) * step) - 1;
            this.partitions[i] = new Partition(low, high);
            this.partitions[i].node = nodes.get(i % nodes.size());
            this.nodePartitions[i % nodes.size()]++;
        }
    }

    @Override
    public RebalanceResult addNodes(List<Node> newNodes) {
        this.nodes.addAll(newNodes);
        this.nodePartitions = Arrays.copyOf(nodePartitions, nodes.size());

        int partitionsPerNodeLow = partitions.length / nodes.size();
        int partitionsPerNodeHigh = partitionsPerNodeLow + ((partitions.length % nodes.size() != 0) ? 1 : 0);

        int count = 0;
        while (true) {
            int min = Utils.minIndex(nodePartitions);
            int max = Utils.maxIndex(nodePartitions);

            if (nodePartitions[min] >= partitionsPerNodeLow && nodePartitions[max] <= partitionsPerNodeHigh) {
                // we have ready reached the steady state
                break;
            }
            if (nodePartitions[min] < partitionsPerNodeHigh && nodePartitions[max] > partitionsPerNodeLow) {
                int moved = Math.min(nodePartitions[max] - partitionsPerNodeLow,
                        partitionsPerNodeHigh - nodePartitions[min]);
                Preconditions.checkState(moved > 0);
                nodePartitions[max] -= moved;
                nodePartitions[min] += moved;
                count += (DistributionSimulator.MAX_KEY / partitions.length) * moved;
                int remaining = moved;
                for (int i = 0; i < partitions.length && remaining > 0; i++) {
                    if (partitions[i].node == nodes.get(max)) {
                        partitions[i].node = nodes.get(min);
                        remaining--;
                    }
                }
            } else {
                throw new IllegalStateException(
                        String.format("Illegal partition assignment. min partitions %d max partitions %d",
                                nodePartitions[min], nodePartitions[max]));
            }
        }

        return new RebalanceResult(count,
                Utils.minValue(nodePartitions) * (DistributionSimulator.MAX_KEY / partitions.length),
                Utils.maxValue(nodePartitions) * (DistributionSimulator.MAX_KEY / partitions.length));
    }

    @Override
    public RebalanceResult removeNodes(List<Node> oldNodes) {
        IntArrayList oldNodeIndex = new IntArrayList();

        int numNodes = nodes.size() - oldNodes.size();
        int[] newNodePartitions = new int[numNodes];
        int pos = 0;
        for (int i = 0; i < nodes.size(); i++) {
            int index = oldNodes.indexOf(nodes.get(i));
            if (index >= 0) {
                oldNodeIndex.add(i);
            } else {
                newNodePartitions[pos++] = nodePartitions[i];
            }
        }

        // now we need to move partition from deleted nodes

        int partitionsPerNodeLow = partitions.length / numNodes;
        int partitionsPerNodeHigh = partitionsPerNodeLow + ((partitions.length % numNodes != 0) ? 1 : 0);

        int count = 0;
        for (int index : oldNodeIndex) {
            while (nodePartitions[index] > 0) {
                int min = Utils.minIndex(newNodePartitions);
                Preconditions.checkState(newNodePartitions[min] < partitionsPerNodeHigh);
                int moved = Math.min(nodePartitions[index], partitionsPerNodeHigh - newNodePartitions[min]);
                count += (double) DistributionSimulator.MAX_KEY / partitions.length * moved;
                nodePartitions[index] -= moved;
                newNodePartitions[min] += moved;
                int remaining = moved;
                for (int i = 0; i < partitions.length && remaining > 0; i++) {
                    if (partitions[i].node == nodes.get(index)) {
                        partitions[i].node = nodes.get(min);
                        remaining--;
                    }
                }
            }
        }
        this.nodePartitions = newNodePartitions;
        this.nodes.removeAll(oldNodes);

        double keysPerPartition = (double) DistributionSimulator.MAX_KEY / partitions.length;

        return new RebalanceResult(count, (int) (Utils.minValue(nodePartitions) * keysPerPartition),
                (int) (Utils.maxValue(nodePartitions) * keysPerPartition));
    }

    @Override
    public void load(int key) {
    }

    @Override
    public String toString() {
        return "static-partition";
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}

class DynamicRangePolicy implements DistributionPolicy {
    private final int maxPartitionSize;
    private final Partition searchKey = new Partition(-1, -1);
    private final List<Node> nodes = new ArrayList<>();
    private final List<Partition> partitions = new ArrayList<>();

    public DynamicRangePolicy(int maxPartitionSize, List<Node> nodes) {
        this.maxPartitionSize = maxPartitionSize;
        double step = (double) DistributionSimulator.MAX_KEY / nodes.size();
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            int low = (int) (i * step);
            int high = (int) ((i + 1) * step) - 1;
            Partition partition = new Partition(low, high);
            partitions.add(partition);
            partition.node = node;
            node.assignment.add(partition);
        }
        this.nodes.addAll(nodes);
    }

    private int findPartitionIndex(int key) {
        searchKey.reset(key, key);
        int index = Collections.binarySearch(partitions, searchKey);
        if (index < 0) {
            index = -index - 1;
        }
        return index;
    }

    @Override
    public void load(int key) {
        int index = findPartitionIndex(key);
        Partition partition = partitions.get(index);
        partition.keys.add(key);
        Node node = partition.node;
        node.assignment.computed = false;

        // check whether we need to split
        if (partition.keys.size() > maxPartitionSize) {
            // split
            int middleBound = partition.middleBound();
            Partition leftPartition = new Partition(partition.lowerBound, middleBound);
            leftPartition.node = partition.node;
            Partition rightPartition = new Partition(middleBound + 1, partition.upperBound);
            rightPartition.node = partition.node;

            for (int k : partition.keys) {
                if (k <= middleBound) {
                    leftPartition.keys.add(k);
                } else {
                    rightPartition.keys.add(k);
                }
            }

            partitions.set(index, leftPartition);
            partitions.add(index + 1, rightPartition);
            node.assignment.remove(partition);
            node.assignment.add(leftPartition);
            node.assignment.add(rightPartition);
            rebalance();
        }
    }

    @Override
    public RebalanceResult addNodes(List<Node> nodes) {
        this.nodes.addAll(nodes);
        return rebalance();
    }

    @Override
    public RebalanceResult removeNodes(List<Node> removed) {
        List<Partition> partitions = new ArrayList<>();
        for (Node node : removed) {
            partitions.addAll(node.assignment.partitions);
        }
        this.nodes.removeAll(removed);
        int count = 0;
        for (Partition partition : partitions) {
            Node minNode = null;
            for (Node node : this.nodes) {
                node.assignment.compute(Assignment.SIZE_SORTER);
                if (minNode == null || minNode.assignment.totalKeys > node.assignment.totalKeys) {
                    minNode = node;
                }
            }
            minNode.assignment.add(partition);
            partition.node = minNode;
            count += partition.numKeys();
        }

        int minKeys = Integer.MAX_VALUE;
        int maxKeys = -1;
        for (Node node : nodes) {
            node.assignment.compute(Assignment.SIZE_SORTER);
            minKeys = Math.min(minKeys, node.assignment.totalKeys);
            maxKeys = Math.max(maxKeys, node.assignment.totalKeys);
        }

        return new RebalanceResult(count, minKeys, maxKeys);
    }

    private RebalanceResult rebalance() {
        int totalKeys = 0;
        for (Node node : nodes) {
            node.assignment.compute(Assignment.SIZE_SORTER);
            totalKeys += node.assignment.totalKeys;
        }

        int count = 0;
        int keysPerNode = totalKeys / nodes.size();
        while (true) {
            Node minNode = null;
            Node maxNode = null;

            for (Node node : nodes) {
                node.assignment.compute(Assignment.SIZE_SORTER);
                if (minNode == null || minNode.assignment.totalKeys > node.assignment.totalKeys) {
                    minNode = node;
                }
                if (maxNode == null || maxNode.assignment.totalKeys < node.assignment.totalKeys) {
                    maxNode = node;
                }
            }
            // now move the smallest partition from maxNode to minNode to see whether this will make the cluster more balanced
            Preconditions.checkState(maxNode.assignment.totalKeys >= keysPerNode);
            Preconditions.checkState(minNode.assignment.totalKeys <= keysPerNode);

            int oldDiff = maxNode.assignment.totalKeys - minNode.assignment.totalKeys;
            Partition smallest = maxNode.assignment.getSmallestPartition();
            int newDiff = Math.abs((maxNode.assignment.totalKeys - smallest.numKeys())
                    - (minNode.assignment.totalKeys + smallest.numKeys()));

            if (newDiff < oldDiff) {
                // proceed to rebalance
                maxNode.assignment.remove(smallest);
                minNode.assignment.add(smallest);
                count += smallest.numKeys();
                smallest.node = minNode;
            } else {
                // no need to rebalance
                break;
            }
        }

        int minKeys = Integer.MAX_VALUE;
        int maxKeys = -1;
        for (Node node : nodes) {
            node.assignment.compute(Assignment.SIZE_SORTER);
            minKeys = Math.min(minKeys, node.assignment.totalKeys);
            maxKeys = Math.max(maxKeys, node.assignment.totalKeys);
        }

        return new RebalanceResult(count, minKeys, maxKeys);
    }

    @Override
    public String toString() {
        return "dynamic-partition";
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}

class ConsistentHashPolicy implements DistributionPolicy {

    private final List<Node> nodes = new ArrayList<>();
    private final List<Partition> partitions = new ArrayList<>();

    private final int partitionsPerNode;

    public ConsistentHashPolicy(int partitionsPerNode, List<Node> nodes) {
        this.nodes.addAll(nodes);
        this.partitionsPerNode = partitionsPerNode;
        int numPartitions = partitionsPerNode * nodes.size();
        int step = DistributionSimulator.MAX_KEY / numPartitions;

        for (int i = 0; i < numPartitions; i++) {
            Partition partition = new Partition(i * step, (i + 1) * step - 1);
            partitions.add(partition);
            Node node = this.nodes.get((i % this.nodes.size()));
            node.assignment.add(partition);
            partition.node = node;
        }
    }

    @Override
    public void load(int key) {
        // no op
    }

    @Override
    public RebalanceResult addNodes(List<Node> nodes) {
        int count = 0;
        PriorityQueue<Node> nodeQueue = new PriorityQueue<>((n1, n2) -> {
            n1.assignment.compute(Assignment.RANGE_SORTER);
            n2.assignment.compute(Assignment.RANGE_SORTER);
            // largest range comes first
            return -Integer.compare(n1.assignment.totalRanges, n2.assignment.totalRanges);
        });
        nodeQueue.addAll(this.nodes);
        for (Node newNode : nodes) {
            // add node one by one
            // add partitions
            for (int i = 0; i < partitionsPerNode; i++) {
                // find the most loaded node

                Node loadedNode = nodeQueue.poll();
                Partition loadedPartition = loadedNode.assignment.partitionList.get(0);

                int partitionIndex = partitions.indexOf(loadedPartition);

                Preconditions.checkState(partitions.size() >= 2);
                Partition prevPartition = partitions.get((partitionIndex + partitions.size() - 1) % partitions.size());
                Partition nextPartition = partitions.get((partitionIndex + 1) % partitions.size());
                prevPartition.node.assignment.compute(Assignment.RANGE_SORTER);
                nextPartition.node.assignment.compute(Assignment.RANGE_SORTER);

                if (prevPartition.node.assignment.totalRanges > nextPartition.node.assignment.totalRanges) {
                    // add the partition before the loadedPartition
                    int middleBound1 =
                            Utils.percentiple(prevPartition.lowerBound, loadedPartition.upperBound, 1.0 / 3.0);
                    int middleBound2 =
                            Utils.percentiple(prevPartition.lowerBound, loadedPartition.upperBound, 2.0 / 3.0);

                    prevPartition.upperBound = middleBound1;
                    prevPartition.node.assignment.computed = false;
                    Partition newPartition = new Partition(middleBound1 + 1, middleBound2);

                    loadedPartition.lowerBound = middleBound2 + 1;
                    loadedPartition.node.assignment.computed = false;

                    newPartition.node = newNode;
                    newNode.assignment.add(newPartition);
                    partitions.add(partitionIndex, newPartition);
                    count += newPartition.range();

                    nodeQueue.remove(prevPartition.node);
                    nodeQueue.add(prevPartition.node);
                } else {
                    int middleBound1 =
                            Utils.percentiple(loadedPartition.lowerBound, nextPartition.upperBound, 1.0 / 3.0);
                    int middleBound2 =
                            Utils.percentiple(loadedPartition.lowerBound, nextPartition.upperBound, 2.0 / 3.0);

                    loadedPartition.upperBound = middleBound1;
                    loadedPartition.node.assignment.computed = false;
                    Partition newPartition = new Partition(middleBound1 + 1, middleBound2);

                    nextPartition.lowerBound = middleBound2 + 1;
                    nextPartition.node.assignment.computed = false;

                    newPartition.node = newNode;
                    newNode.assignment.add(newPartition);
                    partitions.add(partitionIndex + 1, newPartition);
                    count += newPartition.range();

                    nodeQueue.remove(nextPartition.node);
                    nodeQueue.add(nextPartition.node);
                }
                loadedNode.assignment.computed = false;
                nodeQueue.add(loadedNode);
            }
            newNode.assignment.computed = false;
            nodeQueue.add(newNode);
        }

        this.nodes.addAll(nodes);

        int minKeys = Integer.MAX_VALUE;
        int maxKeys = Integer.MIN_VALUE;
        for (Node node : this.nodes) {
            node.assignment.compute(Assignment.RANGE_SORTER);
            minKeys = Math.min(minKeys, node.assignment.totalRanges);
            maxKeys = Math.max(maxKeys, node.assignment.totalRanges);
        }
        return new RebalanceResult(count, minKeys, maxKeys);
    }

    @Override
    public RebalanceResult removeNodes(List<Node> nodes) {
        // remove nodes one by one
        int count = 0;
        for (Node oldNode : nodes) {
            for (Partition oldPartition : oldNode.assignment.partitions) {
                int index = partitions.indexOf(oldPartition);

                Partition prevPartition = partitions.get((index + partitions.size() - 1) % partitions.size());
                Partition nextPartition = partitions.get((index + 1) % partitions.size());

                int middleBound = Utils.percentiple(prevPartition.lowerBound, nextPartition.upperBound, 0.5);
                prevPartition.upperBound = middleBound;
                prevPartition.node.assignment.computed = false;
                nextPartition.lowerBound = middleBound + 1;
                nextPartition.node.assignment.computed = false;
                partitions.remove(index);

                count += oldPartition.range();
            }
        }
        this.nodes.removeAll(nodes);
        int minKeys = Integer.MAX_VALUE;
        int maxKeys = Integer.MIN_VALUE;
        for (Node node : this.nodes) {
            node.assignment.compute(Assignment.RANGE_SORTER);
            minKeys = Math.min(minKeys, node.assignment.totalRanges);
            maxKeys = Math.max(maxKeys, node.assignment.totalRanges);
        }
        return new RebalanceResult(count, minKeys, maxKeys);
    }

    @Override
    public String toString() {
        return "consistent-hashing";
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}
