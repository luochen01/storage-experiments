package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

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

class AggregateRebalanceResult {
    int count;
    double movement;
    double minKeys;
    double maxKeys;

    public void add(RebalanceResult result) {
        count++;
        movement += result.movement;
        minKeys += result.minKeys;
        maxKeys += result.maxKeys;
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
    private IntList loadedKeys = new IntArrayList();

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
        for (int k : loadedKeys) {
            Node node1 = oldNodes.get(k % oldNodes.size());
            Node node2 = newNodes.get(k % newNodes.size());
            if (node1.id != node2.id) {
                count++;
            }
            keysPerNode[k % newNodes.size()]++;
        }
        return new RebalanceResult(count, Utils.minValue(keysPerNode), Utils.maxValue(keysPerNode));
    }

    @Override
    public void load(int key) {
        loadedKeys.add(key);
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
    private final IntList loadedKeys = new IntArrayList();

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
        Preconditions.checkState(newPolicy.nodes.size() == this.nodes.size() - nodes.size());
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
        for (int k : loadedKeys) {
            Node node1 = this.assign(k);
            Node node2 = newPolicy.assign(k);
            if (node1 != node2) {
                count++;
            }
            keysPerNode[newPolicy.assignIndex(k)]++;
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
        loadedKeys.add(key);
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
    private final IntList loadedKeys = new IntArrayList();

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
        loadedKeys.sort(Integer::compare);
        for (int key : loadedKeys) {
            while (key > oldUpperBounds[pos1]) {
                pos1++;
            }
            while (key > newUpperBounds[pos2]) {
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
        loadedKeys.add(key);
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
    private final Partition[] partitions;

    public StaticPartitionPolicy(int partitionsPerNode, List<Node> nodes) {
        int numPartitions = partitionsPerNode * nodes.size();
        this.partitions = new Partition[numPartitions];
        this.nodes.addAll(nodes);
        for (int i = 0; i < numPartitions; i++) {
            this.partitions[i] = new Partition(Integer.MIN_VALUE, Integer.MAX_VALUE);
            Node node = nodes.get(i % nodes.size());
            node.assignment.add(this.partitions[i]);
        }
    }

    @Override
    public RebalanceResult addNodes(List<Node> newNodes) {
        this.nodes.addAll(newNodes);

        int count = 0;
        while (true) {
            Node minNode = Utils.minKeysNode(nodes);
            Node maxNode = Utils.maxKeysNode(nodes);

            int oldDiff = maxNode.assignment.getTotalKeys() - minNode.assignment.getTotalKeys();
            Partition smallest = maxNode.assignment.getPartitionList(Assignment.SIZE_SORTER).get(0);
            int newDiff = Math.abs((maxNode.assignment.getTotalKeys() - smallest.numKeys())
                    - (minNode.assignment.getTotalKeys() + smallest.numKeys()));

            if (newDiff < oldDiff) {
                // proceed to rebalance
                maxNode.assignment.remove(smallest);
                minNode.assignment.add(smallest);
                count += smallest.numKeys();
            } else {
                // no need to rebalance
                break;
            }
        }
        return new RebalanceResult(count, Utils.minKeysNode(nodes).numKeys(), Utils.maxKeysNode(nodes).numKeys());
    }

    @Override
    public RebalanceResult removeNodes(List<Node> oldNodes) {
        this.nodes.removeAll(oldNodes);
        int numNodes = nodes.size();

        // now we need to move partition from deleted nodes

        int partitionsPerNodeLow = partitions.length / numNodes;
        int partitionsPerNodeHigh = partitionsPerNodeLow + ((partitions.length % numNodes != 0) ? 1 : 0);

        int count = 0;
        for (Node oldNode : oldNodes) {
            while (oldNode.numPartitions() > 0) {
                Node minNode = Utils.minPartitionsNode(nodes);
                Preconditions.checkState(minNode.numPartitions() < partitionsPerNodeHigh);
                int moved = Math.min(oldNode.numPartitions(), partitionsPerNodeHigh - minNode.numPartitions());
                int remaining = moved;
                for (int i = 0; i < partitions.length && remaining > 0; i++) {
                    if (partitions[i].getNode() == oldNode) {
                        oldNode.assignment.remove(partitions[i]);
                        minNode.assignment.add(partitions[i]);
                        count += partitions[i].numKeys();
                        remaining--;
                    }
                }
            }
        }

        return new RebalanceResult(count, Utils.minKeysNode(nodes).numKeys(), Utils.maxKeysNode(nodes).numKeys());
    }

    @Override
    public void load(int key) {
        Partition partition = partitions[key % partitions.length];
        partition.addKey(key);
    }

    @Override
    public String toString() {
        return "static-partition-" + partitions.length / nodes.size();
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}

class DynamicPartitionPolicy implements DistributionPolicy {
    private final int maxPartitionSize;
    private final Partition searchKey = new Partition(-1, -1);
    private final List<Node> nodes = new ArrayList<>();
    private final List<Partition> partitions = new ArrayList<>();

    public DynamicPartitionPolicy(int maxPartitionSize, List<Node> nodes) {
        searchKey.setNode(new Node(-1));
        this.maxPartitionSize = maxPartitionSize;
        double step = (double) DistributionSimulator.MAX_KEY / nodes.size();
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            int low = (int) (i * step);
            int high = (int) ((i + 1) * step) - 1;
            Partition partition = new Partition(low, high);
            partitions.add(partition);
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
        partition.addKey(key);

        // check whether we need to split
        if (partition.numKeys() > maxPartitionSize) {
            // split
            int middleBound = partition.middleBound();
            Node node = partition.getNode();
            Partition leftPartition = new Partition(partition.getLowerBound(), middleBound);
            node.assignment.add(leftPartition);
            Partition rightPartition = new Partition(middleBound + 1, partition.getUpperBound());
            node.assignment.add(rightPartition);

            node.assignment.remove(partition);

            partitions.set(index, leftPartition);
            partitions.add(index + 1, rightPartition);

            partition.forEach(k -> {
                if (k <= middleBound) {
                    leftPartition.addKey(k);
                } else {
                    rightPartition.addKey(k);
                }
            });
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
            partition.getNode().assignment.remove(partition);
            Node minNode = Utils.minKeysNode(nodes);
            minNode.assignment.add(partition);
            count += partition.numKeys();
        }

        return new RebalanceResult(count, Utils.minKeysNode(nodes).numKeys(), Utils.maxKeysNode(nodes).numKeys());
    }

    private RebalanceResult rebalance() {
        int totalKeys = Utils.getTotalKeys(nodes);

        int count = 0;
        int keysPerNode = totalKeys / nodes.size();
        while (true) {
            Node minNode = Utils.minKeysNode(nodes);
            Node maxNode = Utils.maxKeysNode(nodes);
            // now move the smallest partition from maxNode to minNode to see whether this will make the cluster more balanced
            Preconditions.checkState(maxNode.assignment.getTotalKeys() >= keysPerNode);
            Preconditions.checkState(minNode.assignment.getTotalKeys() <= keysPerNode);

            int oldDiff = maxNode.assignment.getTotalKeys() - minNode.assignment.getTotalKeys();
            Partition smallest = maxNode.assignment.getPartitionList(Assignment.SIZE_SORTER).get(0);
            int newDiff = Math.abs((maxNode.assignment.getTotalKeys() - smallest.numKeys())
                    - (minNode.assignment.getTotalKeys() + smallest.numKeys()));

            if (newDiff < oldDiff) {
                // proceed to rebalance
                maxNode.assignment.remove(smallest);
                minNode.assignment.add(smallest);
                count += smallest.numKeys();
            } else {
                // no need to rebalance
                break;
            }
        }

        return new RebalanceResult(count, Utils.minKeysNode(nodes).numKeys(), Utils.maxKeysNode(nodes).numKeys());
    }

    @Override
    public String toString() {
        return "dynamic-partition-" + maxPartitionSize;
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}

class ConsistentHashPolicy implements DistributionPolicy {

    private final Partition searchKey = new Partition(-1, -1);

    private final List<Node> nodes = new ArrayList<>();
    private final List<Partition> partitions = new ArrayList<>();

    private final int partitionsPerNode;

    public ConsistentHashPolicy(int partitionsPerNode, List<Node> nodes) {
        searchKey.setNode(new Node(-1));

        this.nodes.addAll(nodes);
        this.partitionsPerNode = partitionsPerNode;
        int numPartitions = partitionsPerNode * nodes.size();
        double step = DistributionSimulator.MAX_KEY / numPartitions;

        List<Node> tmp = new ArrayList<>(this.nodes);
        int pos = 0;
        Collections.shuffle(tmp);
        for (int i = 0; i < numPartitions; i++) {
            Partition partition = new Partition((int) (i * step), (int) ((i + 1) * step - 1));
            partitions.add(partition);
            Node node = tmp.get(pos++);
            if (pos == tmp.size()) {
                pos = 0;
                Collections.shuffle(tmp);
            }
            node.assignment.add(partition);
        }
        partitions.get(partitions.size() - 1).resetUpper(DistributionSimulator.MAX_KEY - 1);
    }

    @Override
    public void load(int key) {
        // no op
        if (partitions.get(0).inRange(key)) {
            partitions.get(0).addKey(key);
        } else if (partitions.get(partitions.size() - 1).inRange(key)) {
            partitions.get(partitions.size() - 1).addKey(key);
        } else {
            searchKey.reset(key, key);
            int index = indexedBinarySearch(partitions, searchKey);
            if (index < 0) {
                index = -index - 1;
            }
            partitions.get(index).addKey(key);
        }
    }

    private int indexedBinarySearch(List<Partition> list, Partition key) {
        int low = 1;
        int high = list.size() - 2;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Partition midVal = list.get(mid);
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1); // key not found
    }

    @Override
    public RebalanceResult addNodes(List<Node> addedNodes) {
        int count = 0;
        PriorityQueue<Node> nodeQueue = new PriorityQueue<>((n1, n2) -> {
            // largest nodes comes first
            return -Integer.compare(n1.assignment.getTotalKeys(), n2.assignment.getTotalKeys());
        });
        nodeQueue.addAll(this.nodes);
        for (Node newNode : addedNodes) {
            // add node one by one
            // add partitions
            for (int i = 0; i < partitionsPerNode; i++) {
                // find the most loaded node

                Node loadedNode = nodeQueue.poll();
                Partition loadedPartition = loadedNode.assignment.getLastPartition(Assignment.SIZE_SORTER);

                int partitionIndex = partitions.indexOf(loadedPartition);
                int middleBound = loadedPartition.middleBound();

                Partition newLoadedPartition = new Partition(loadedPartition.getLowerBound(), middleBound);
                Partition newPartition = new Partition((middleBound + 1) % DistributionSimulator.MAX_KEY,
                        loadedPartition.getUpperBound());
                loadedNode.assignment.remove(loadedPartition);
                loadedNode.assignment.add(newLoadedPartition);
                newNode.assignment.add(newPartition);
                partitions.set(partitionIndex, newLoadedPartition);
                partitions.add(partitionIndex + 1, newPartition);

                loadedPartition.forEach(k -> {
                    if (newLoadedPartition.inRange(k)) {
                        newLoadedPartition.addKey(k);
                    } else {
                        newPartition.addKey(k);
                    }
                });
                count += newPartition.numKeys();
                nodeQueue.add(loadedNode);
            }
            nodeQueue.add(newNode);
        }

        this.nodes.addAll(addedNodes);
        return new RebalanceResult(count, Utils.minKeysNode(this.nodes).numKeys(),
                Utils.maxKeysNode(this.nodes).numKeys());
    }

    @Override
    public RebalanceResult removeNodes(List<Node> oldNodes) {
        // remove nodes one by one
        int count = 0;
        for (Node oldNode : oldNodes) {
            for (Partition oldPartition : oldNode.assignment.partitions) {
                int index = partitions.indexOf(oldPartition);

                Partition prevPartition = partitions.get((index + partitions.size() - 1) % partitions.size());
                prevPartition.resetUpper(oldPartition.getUpperBound());
                oldPartition.forEach(k -> prevPartition.addKey(k));
                partitions.remove(index);

                prevPartition.getNode().assignment.invalidateAll();

                count += oldPartition.numKeys();
            }
        }
        this.nodes.removeAll(oldNodes);
        return new RebalanceResult(count, Utils.minKeysNode(this.nodes).numKeys(),
                Utils.maxKeysNode(this.nodes).numKeys());
    }

    @Override
    public String toString() {
        return "consistent-hashing-" + partitionsPerNode;
    }

    @Override
    public List<Node> getNodes() {
        return nodes;
    }

}
