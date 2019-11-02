package edu.uci.asterixdb.storage.sim;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

interface StorageUnit extends Comparable<StorageUnit> {
    long min();

    long max();

    int getSize();

    int getDirtySize();

    boolean isPersisted();

    @Override
    default int compareTo(StorageUnit o) {
        if (max() < o.min()) {
            return -1;
        } else if (min() > o.max()) {
            return 1;
        } else {
            return 0;
        }
    }
}

class SSTable implements StorageUnit {
    final boolean isMemory;
    final long[] keys;
    final long[] seqs;
    long minSeq;
    private int numKeys;
    private int numDirtyKeys;
    boolean isFree;

    boolean isPersisted;

    public SSTable(int capacity, boolean isMemory) {
        numKeys = 0;
        this.keys = new long[capacity];
        this.seqs = new long[capacity];

        this.isMemory = isMemory;
    }

    public void reset() {
        this.numKeys = 0;
        this.numDirtyKeys = 0;
        this.minSeq = Long.MAX_VALUE;
        this.isPersisted = false;
    }

    public void resetKey(long key) {
        this.numKeys = 1;
        this.keys[0] = key;
    }

    public void resetRange() {
        this.numKeys = 2;
        this.keys[0] = Long.MAX_VALUE;
        this.keys[1] = Long.MIN_VALUE;
    }

    public void updateRange(StorageUnit sstable) {
        if (this.numKeys != 2) {
            throw new IllegalStateException();
        }
        this.keys[0] = Math.min(this.keys[0], sstable.min());
        this.keys[1] = Math.max(this.keys[1], sstable.max());
    }

    public void add(long key, long seq) {
        keys[numKeys] = key;
        seqs[numKeys] = seq;
        numKeys++;
        if (seq >= 0) {
            this.minSeq = Math.min(minSeq, seq);
            this.numDirtyKeys++;
        }
    }

    public boolean isFull() {
        return numKeys >= keys.length;
    }

    @Override
    public boolean isPersisted() {
        return isPersisted;
    }

    @Override
    public long min() {
        return keys[0];
    }

    @Override
    public long max() {
        return keys[numKeys - 1];
    }

    @Override
    public int getSize() {
        return numKeys;
    }

    @Override
    public int getDirtySize() {
        if (isPersisted) {
            return 0;
        } else {
            return numDirtyKeys;
        }
    }

    @Override
    public String toString() {
        return "[" + min() + "," + max() + "]";
    }
}

class SSTableGroup implements StorageUnit {
    final TreeSet<StorageUnit> sstables = new TreeSet<StorageUnit>();
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private int size = 0;

    public SSTableGroup(SSTable sstable) {
        add(sstable);
    }

    public SSTableGroup(List<SSTable> sstables) {
        sstables.forEach(t -> add(t));
    }

    public void add(StorageUnit sstable) {
        boolean result = sstables.add(sstable);
        assert result == true;
        this.min = Math.min(min, sstable.min());
        this.max = Math.max(max, sstable.max());
        this.size += sstable.getSize();
    }

    public int remove(Set<StorageUnit> sstables) {
        int totalSize = Utils.getTotalSize(sstables);
        int oldSize = this.sstables.size();
        int removeSize = sstables.size();
        sstables.clear();
        int newSize = this.sstables.size();
        assert newSize + removeSize == oldSize;

        if (!sstables.isEmpty()) {
            this.min = this.sstables.first().min();
        }
        this.size -= totalSize;
        return totalSize;
    }

    public int remove(StorageUnit sstable) {
        sstables.remove(sstable);
        this.size -= sstable.getSize();
        return sstable.getSize();

    }

    public void clear() {
        this.size = 0;
        sstables.clear();
    }

    @Override
    public long min() {
        return min;
    }

    @Override
    public long max() {
        return max;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Override
    public int getDirtySize() {
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (StorageUnit sstable : sstables) {
            sb.append(sstable);
        }
        sb.append("}");
        return sb.toString();
    }

}