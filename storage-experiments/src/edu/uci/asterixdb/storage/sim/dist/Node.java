package edu.uci.asterixdb.storage.sim.dist;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

class Partition implements Comparable<Partition> {
    int lowerBound;
    int upperBound;
    final IntSet keys = new IntOpenHashSet();
    Node node;

    public Partition(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void reset(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
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

    public int middleBound() {
        if (lowerBound < upperBound) {
            return (upperBound + lowerBound) / 2;
        } else {
            return (upperBound + lowerBound + DistributionSimulator.MAX_KEY) / 2;
        }
    }

    public int range() {
        if (lowerBound < upperBound) {
            return upperBound - lowerBound + 1;
        } else {
            return upperBound - lowerBound + 1 + DistributionSimulator.MAX_KEY;
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
    public static final Comparator<Partition> SIZE_SORTER =
            (p1, p2) -> -Integer.compare(p1.keys.size(), p2.keys.size());

    public static final Comparator<Partition> RANGE_SORTER = (p1, p2) -> -Integer.compare(p1.range(), p2.range());

    final Set<Partition> partitions = new HashSet<>();
    // must be computed before each rebalance

    boolean computed = false;
    int totalKeys;
    int totalRanges;
    final List<Partition> partitionList = new ArrayList<>();

    public void add(Partition p) {
        partitions.add(p);
        computed = false;
    }

    public void remove(Partition p) {
        partitions.remove(p);
        computed = false;
    }

    public Partition getSmallestPartition() {
        Preconditions.checkState(computed);
        return partitionList.get(partitionList.size() - 1);
    }

    public void compute(Comparator<Partition> sorter) {
        if (computed) {
            return;
        }
        totalRanges = 0;
        totalKeys = 0;
        partitionList.clear();
        for (Partition partition : partitions) {
            totalKeys += partition.keys.size();
            totalRanges += partition.range();
            partitionList.add(partition);
        }

        // order from largest to smallest
        partitionList.sort(sorter);
        computed = true;
    }
}

class Node {
    public final int id;
    public final IntSet keys = new IntOpenHashSet();
    public final Assignment assignment = new Assignment();

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
}
