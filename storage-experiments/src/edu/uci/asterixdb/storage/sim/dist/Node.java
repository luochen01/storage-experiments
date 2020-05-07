package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

class Partition implements Comparable<Partition> {
    private int lowerBound;
    private int upperBound;
    private final IntSet keys = new IntOpenHashSet();
    private Node node;

    public Partition(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public void forEach(IntConsumer action) {
        keys.forEach(action);
    }

    public void reset(int lower, int upper) {
        this.lowerBound = lower;
        this.upperBound = upper;
    }

    public void resetLower(int lowerBound) {
        this.lowerBound = lowerBound;
    }

    public void resetUpper(int upperBound) {
        this.upperBound = upperBound;
    }

    public void addKey(int key) {
        Preconditions.checkState(inRange(key));
        keys.add(key);
        node.assignment.invalidateKeys();
    }

    @Override
    public int compareTo(Partition o) {
        if (upperBound < o.lowerBound) {
            return -1;
        } else if (lowerBound > o.upperBound) {
            return 1;
        } else {
            return 0;
        }
    }

    public boolean inRange(int key) {
        if (lowerBound < upperBound) {
            return lowerBound <= key && key <= upperBound;
        } else {
            return lowerBound <= key || key <= upperBound;
        }
    }

    public int getLowerBound() {
        return lowerBound;
    }

    public int getUpperBound() {
        return upperBound;
    }

    public int middleBound() {
        if (lowerBound < upperBound) {
            return (upperBound + lowerBound) / 2;
        } else {
            return (upperBound + lowerBound + DistributionSimulator.MAX_KEY) / 2;
        }
    }

    @Override
    public String toString() {
        return String.format("[%d,%d]", lowerBound, upperBound);
    }

    public int numKeys() {
        return keys.size();
    }
}

class Assignment {
    public static final Comparator<Partition> SIZE_SORTER = (p1, p2) -> Integer.compare(p1.numKeys(), p2.numKeys());

    final Set<Partition> partitions = new HashSet<>();
    // must be computed before each rebalance

    private final Node node;
    private boolean keyComputed = false;
    private int totalKeys;
    private boolean listComputed = false;
    private final List<Partition> partitionList = new ArrayList<>();

    public Assignment(Node node) {
        this.node = node;
    }

    public void add(Partition p) {
        Preconditions.checkState(p.getNode() == null);
        partitions.add(p);
        p.setNode(node);
        invalidateAll();
    }

    public void invalidateAll() {
        keyComputed = false;
        listComputed = false;
    }

    public void remove(Partition p) {
        Preconditions.checkState(p.getNode() == node);
        partitions.remove(p);
        p.setNode(null);
        invalidateAll();
    }

    public void invalidateKeys() {
        keyComputed = false;
    }

    public List<Partition> getPartitionList(Comparator<Partition> sorter) {
        computePartitionList(sorter);
        return partitionList;
    }

    public Partition getLastPartition(Comparator<Partition> sorter) {
        computePartitionList(sorter);
        return partitionList.get(partitionList.size() - 1);
    }

    private void computePartitionList(Comparator<Partition> sorter) {
        if (!listComputed) {
            partitionList.clear();
            partitionList.addAll(partitions);
            if (sorter != null) {
                partitionList.sort(sorter);
            }
            listComputed = true;
        }
    }

    public int getTotalKeys() {
        if (!keyComputed) {
            totalKeys = 0;
            for (Partition partition : partitions) {
                totalKeys += partition.numKeys();
            }
            keyComputed = true;
        }
        return totalKeys;
    }
}

class Node {
    public final int id;
    public final IntSet keys = new IntOpenHashSet();
    public final Assignment assignment = new Assignment(this);

    public Node(int id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
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
        Node other = (Node) obj;
        if (id != other.id)
            return false;
        return true;
    }

    public int numPartitions() {
        return assignment.partitions.size();
    }

    public int numKeys() {
        return assignment.getTotalKeys();
    }

}
