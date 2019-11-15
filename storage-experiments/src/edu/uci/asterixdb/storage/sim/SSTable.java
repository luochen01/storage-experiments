package edu.uci.asterixdb.storage.sim;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

abstract class SSTable implements Comparable<SSTable> {
    final int[] keys;
    protected int numKeys;
    boolean isFree;

    public int getSize() {
        return numKeys;
    }

    public int min() {
        return keys[0];
    }

    public int getKey(int index) {
        return keys[index];
    }

    public int getSeq(int index) {
        return 0;
    }

    public int getMinSeq() {
        return 0;
    }

    public void readAll() {
        // no op
    }

    public int max() {
        return keys[numKeys - 1];
    }

    public SSTable(int capacity) {
        this.keys = new int[capacity];
    }

    public void reset() {
        numKeys = 0;
    }

    public boolean isFull() {
        return numKeys >= keys.length;
    }

    public void write() {

    }

    public abstract void add(int key, int seq);

    public abstract boolean contains(int key);

    @Override
    public int compareTo(SSTable o) {
        if (max() < o.min()) {
            return -1;
        } else if (min() > o.max()) {
            return 1;
        } else {
            return 0;
        }
    }

}

class MemorySSTable extends SSTable {
    final int[] seqs;
    int minSeq;

    public MemorySSTable(int capacity) {
        super(capacity);
        this.seqs = new int[capacity];
    }

    @Override
    public int getSeq(int index) {
        return seqs[index];
    }

    @Override
    public void reset() {
        super.reset();
        this.minSeq = Integer.MAX_VALUE;
    }

    @Override
    public boolean contains(int key) {
        return Arrays.binarySearch(seqs, 0, numKeys, key) >= 0;
    }

    @Override
    public void add(int key, int seq) {
        keys[numKeys] = key;
        seqs[numKeys] = seq;
        assert seq >= 0;
        numKeys++;
        minSeq = Math.min(minSeq, seq);
    }

    @Override
    public boolean isFull() {
        return numKeys >= keys.length;
    }

    @Override
    public int getSize() {
        return numKeys;
    }

    @Override
    public int getMinSeq() {
        return minSeq;
    }

    @Override
    public String toString() {
        return "[" + min() + "," + max() + "]";
    }
}

class DiskSSTable extends SSTable {
    final Page[] pages;
    final Page[] bfPages;
    final LSMSimulator sim;

    public DiskSSTable(int capacity, LSMSimulator sim) {
        super(capacity);
        pages = new Page[Utils.ceil(capacity, sim.config.tuningConfig.pageSize)];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = new Page();
        }
        // suppose each key is 1KB. each page is 1KB * pageSize
        // each key has 10 bits (10/8 bytes).
        this.bfPages = new Page[Utils.getBloomFilterPages(capacity, sim.config.tuningConfig.pageSize)];
        for (int i = 0; i < bfPages.length; i++) {
            this.bfPages[i] = new Page();
        }
        this.sim = sim;
    }

    @Override
    public boolean contains(int key) {
        int bfIndex = (int) ((double) (key - min()) / (max() - min() + 1) * bfPages.length);
        sim.cache.pin(bfPages[bfIndex]);

        int index = Arrays.binarySearch(keys, 0, numKeys, key);
        if (index >= 0) {
            sim.cache.pin(pages[index / sim.cache.getPageSize()]);
            return true;
        } else {
            index = -index - 1;
            if (sim.rand.nextInt(100) == 0) {
                sim.cache.pin(pages[index / numKeys]);
            }
            return false;
        }
    }

    @Override
    public void add(int key, int seq) {
        keys[numKeys++] = key;
    }

    @Override
    public void write() {
        sim.cache.write(getNumPages());
        sim.cache.write(getNumBFPages());
    }

    // for merge
    @Override
    public void readAll() {
        int numPages = getNumPages();
        for (int i = 0; i < numPages; i++) {
            sim.cache.read(pages[i]);
        }
    }

    public int getNumPages() {
        return Utils.ceil(numKeys, sim.cache.getPageSize());
    }

    public int getNumBFPages() {
        return Utils.getBloomFilterPages(numKeys, sim.cache.getPageSize());
    }

    @Override
    public void reset() {
        super.reset();
        int numPages = getNumPages();
        for (int i = 0; i < numPages; i++) {
            sim.cache.evict(pages[i]);
        }
        int numBFPages = getNumBFPages();
        for (int i = 0; i < numBFPages; i++) {
            sim.cache.evict(bfPages[i]);
        }
    }

}

class KeySSTable extends SSTable {

    public KeySSTable() {
        super(2);
    }

    public void resetKey(int key) {
        this.numKeys = 1;
        this.keys[0] = key;
    }

    public void resetRange() {
        this.numKeys = 2;
        this.keys[0] = Integer.MAX_VALUE;
        this.keys[1] = Integer.MIN_VALUE;
    }

    public void updateRange(SSTable sstable) {
        if (this.numKeys != 2) {
            throw new IllegalStateException();
        }
        this.keys[0] = Math.min(this.keys[0], sstable.min());
        this.keys[1] = Math.max(this.keys[1], sstable.max());
    }

    @Override
    public void add(int key, int seq) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(int key) {
        throw new UnsupportedOperationException();
    }

}

class SSTableGroup {
    final TreeSet<SSTable> sstables = new TreeSet<SSTable>();
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private int size = 0;

    public SSTableGroup(SSTable sstable) {
        add(sstable);
    }

    public SSTableGroup(List<SSTable> sstables) {
        sstables.forEach(t -> add(t));
    }

    public void add(SSTable sstable) {
        boolean result = sstables.add(sstable);
        assert result == true;
        this.min = Math.min(min, sstable.min());
        this.max = Math.max(max, sstable.max());
        this.size += sstable.getSize();
    }

    public int remove(Set<SSTable> sstables) {
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

    public int remove(SSTable sstable) {
        sstables.remove(sstable);
        this.size -= sstable.getSize();
        return sstable.getSize();

    }

    public void clear() {
        this.size = 0;
        sstables.clear();
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (SSTable sstable : sstables) {
            sb.append(sstable);
        }
        sb.append("}");
        return sb.toString();
    }

}