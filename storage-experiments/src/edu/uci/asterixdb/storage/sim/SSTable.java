package edu.uci.asterixdb.storage.sim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import edu.uci.asterixdb.storage.sim.cache.Page;
import edu.uci.asterixdb.storage.sim.cache.Page.PageState;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

abstract class SSTable implements Comparable<SSTable> {
    final int[] keys;
    final Int2IntMap keyMap = new Int2IntOpenHashMap();
    protected int numKeys;
    boolean isFree;
    int level;
    protected SimulatedLSM lsm;

    public SSTable(int capacity) {
        this.keys = new int[capacity];
        keyMap.defaultReturnValue(-1);
    }

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

    public boolean isCached(int keyIndex) {
        return true;
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

    public void reset(int level) {
        numKeys = 0;
        keyMap.clear();
        this.level = level;
    }

    public void deletePages() {

    }

    public boolean isFull() {
        return numKeys >= keys.length;
    }

    public void endBulkLoad(boolean isFlush) {

    }

    public void serialize(DataOutput output) throws IOException {
        output.writeInt(numKeys);
        for (int i = 0; i < numKeys; i++) {
            output.writeInt(keys[i]);
        }
    }

    public void deserialize(DataInput input, int level) throws IOException {
        reset(level);
        int keys = input.readInt();
        for (int i = 0; i < keys; i++) {
            add(input.readInt(), 0, false, true);
        }
    }

    public abstract void add(int key, int seq, boolean cached, boolean load);

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isFree ? 1231 : 1237);
        result = prime * result + ((keyMap == null) ? 0 : keyMap.hashCode());
        result = prime * result + Arrays.hashCode(keys);
        result = prime * result + numKeys;
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
        SSTable other = (SSTable) obj;
        if (isFree != other.isFree)
            return false;
        if (keyMap == null) {
            if (other.keyMap != null)
                return false;
        } else if (!keyMap.equals(other.keyMap))
            return false;
        if (!Arrays.equals(keys, other.keys))
            return false;
        if (numKeys != other.numKeys)
            return false;
        return true;
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
    public void reset(int level) {
        super.reset(level);
        this.minSeq = Integer.MAX_VALUE;
    }

    @Override
    public boolean contains(int key) {
        return keyMap.containsKey(key);
    }

    @Override
    public void add(int key, int seq, boolean cached, boolean load) {
        keys[numKeys] = key;
        seqs[numKeys] = seq;
        keyMap.put(key, numKeys);
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
    final Simulator sim;
    final boolean[] cached;

    public DiskSSTable(int capacity, Simulator sim) {
        super(capacity);
        pages = new Page[Utils.ceil(capacity, sim.config.tuningConfig.pageSize)];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = new Page();
        }
        cached = new boolean[pages.length];
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
        assert !isFree;
        int bfIndex = (int) ((double) (key - min()) / (max() - min() + 1) * getNumBFPages());
        sim.cache.access(bfPages[bfIndex]);

        int index = keyMap.get(key);
        if (index >= 0) {
            sim.cache.access(pages[index / sim.config.tuningConfig.pageSize]);
            return true;
        } else {
            if (sim.rand.nextInt(100) == 0) {
                int pageIndex = (int) ((double) (key - min()) / (max() - min() + 1) * getNumPages());
                sim.cache.access(pages[pageIndex]);
            }
            return false;
        }
    }

    @Override
    public void add(int key, int seq, boolean cached, boolean load) {
        int index = numKeys;
        keys[index] = key;
        keyMap.put(key, index);
        numKeys++;
        if (!load && pages[index / sim.config.tuningConfig.pageSize].state != PageState.CACHED) {
            if (cached || level < lsm.diskLevels.size() - 1) {
                sim.cache.mergeReturnPage(pages[index / sim.config.tuningConfig.pageSize]);
            }
        }
    }

    @Override
    public void endBulkLoad(boolean isFlush) {
        if (isFlush) {
            sim.cache.flushWrite(getNumPages());
            sim.cache.flushWrite(getNumBFPages());
        } else {
            sim.cache.mergeWrite(getNumPages());
            sim.cache.mergeWrite(getNumBFPages());

            lsm.stats.mergeDiskWrites += getNumPages();
            lsm.stats.mergeDiskWrites += getNumBFPages();
        }

    }

    // for merge
    @Override
    public void readAll() {
        int numPages = getNumPages();
        for (int i = 0; i < numPages; i++) {
            cached[i] = pages[i].state == PageState.CACHED;
            sim.cache.mergeReadPage(pages[i]);
        }
    }

    public int getNumPages() {
        return Utils.ceil(numKeys, sim.config.tuningConfig.pageSize);
    }

    public int getNumBFPages() {
        return Utils.getBloomFilterPages(numKeys, sim.config.tuningConfig.pageSize);
    }

    @Override
    public boolean isCached(int keyIndex) {
        return cached[keyIndex / sim.config.tuningConfig.pageSize];
    }

    @Override
    public void deletePages() {
        super.deletePages();
        int numPages = getNumPages();
        for (int i = 0; i < numPages; i++) {
            sim.cache.delete(pages[i]);
        }
        int numBFPages = getNumBFPages();
        for (int i = 0; i < numBFPages; i++) {
            sim.cache.delete(bfPages[i]);
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
    public void add(int key, int seq, boolean cached, boolean load) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(int key) {
        throw new UnsupportedOperationException();
    }

}

class SSTableGroup {
    final TreeSet<SSTable> sstables = new TreeSet<SSTable>();
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private int size = 0;

    public SSTableGroup(SSTable sstable) {
        add(sstable);
    }

    public SSTableGroup(List<SSTable> sstables) {
        sstables.forEach(t -> add(t));
    }

    public void serialize(DataOutput output) throws IOException {
        output.writeInt(min);
        output.writeInt(max);
        output.writeInt(size);
        output.writeInt(sstables.size());
        for (SSTable sstable : sstables) {
            sstable.serialize(output);
        }
    }

    public void deserialize(DataInput input, int level, Simulator sim, SimulatedLSM lsm) throws IOException {
        min = input.readInt();
        max = input.readInt();
        size = input.readInt();
        int num = input.readInt();
        sstables.clear();
        for (int i = 0; i < num; i++) {
            SSTable sstable = sim.getFreeSSTable(lsm, false, level);
            sstable.deserialize(input, level);
            sstables.add(sstable);
        }
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + max;
        result = prime * result + min;
        result = prime * result + size;
        result = prime * result + ((sstables == null) ? 0 : sstables.hashCode());
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
        SSTableGroup other = (SSTableGroup) obj;
        if (max != other.max)
            return false;
        if (min != other.min)
            return false;
        if (size != other.size)
            return false;
        if (sstables == null) {
            if (other.sstables != null)
                return false;
        } else if (!sstables.equals(other.sstables))
            return false;
        return true;
    }

}