package edu.uci.asterixdb.storage.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

abstract class Level {
    final boolean inMemory;
    int level;
    private long size = 0;
    SSTable lastKey = new SSTable(1, false);

    long mergedKeys = 0;
    long overlapingKeys = 0;
    long resultingKeys = 0;

    public Level(int level, boolean inMemory) {
        this.level = level;
        this.inMemory = inMemory;

        lastKey.resetKey(-1);
    }

    public long getSize() {
        return size;
    }

    public void addSize(int value) {
        assert value >= 0;
        size += value;
    }

    public void decrementSize(int value) {
        assert value >= 0;
        size -= value;
        assert size >= 0;
    }

    public void resetStats() {
        this.mergedKeys = 0;
        this.overlapingKeys = 0;
        this.resultingKeys = 0;
    }

}

class PartitionedLevel extends Level {

    final TreeSet<StorageUnit> sstables = new TreeSet<>();

    public PartitionedLevel(int level, boolean isMemory) {
        super(level, isMemory);
    }

    public void remove(StorageUnit sstable) {
        if (!sstables.remove(sstable)) {
            throw new IllegalStateException("removing non-existing sstable " + sstable);
        }
        decrementSize(sstable.getSize());
    }

    public void add(StorageUnit sstable) {
        if (!sstables.add(sstable)) {
            throw new IllegalStateException("adding duplicated sstable " + sstable);
        }
        addSize(sstable.getSize());
    }

    public long replace(Set<StorageUnit> oldSet, List<StorageUnit> newList) {
        long oldSize = getSize();
        remove(oldSet);
        newList.forEach(t -> add(t));
        return getSize() - oldSize;
    }

    private void remove(Set<StorageUnit> oldSet) {
        int totalSize = 0;
        for (StorageUnit unit : oldSet) {
            totalSize += unit.getSize();
        }
        decrementSize(totalSize);

        int oldSize = sstables.size();
        int oldSetSize = oldSet.size();
        oldSet.clear();
        int newSize = sstables.size();
        assert newSize == oldSize - oldSetSize;

    }

}

class UnpartitionedLevel extends Level {

    final List<SSTableGroup> groups = new ArrayList<>();

    public UnpartitionedLevel(int level) {
        super(level, false);
    }

    public void add(SSTableGroup group) {
        groups.add(group);
        addSize(group.getSize());
    }

    public void addGroups(List<SSTableGroup> newGroups) {
        newGroups.forEach(t -> add(t));
    }

    public void clearGroup(SSTableGroup group) {
        decrementSize(group.getSize());
        group.clear();
    }

    public void remove(SSTableGroup group, Set<StorageUnit> sstables) {
        int removedSize = group.remove(sstables);
        decrementSize(removedSize);
    }

    public void remove(SSTableGroup group, StorageUnit sstable) {
        int removedSize = group.remove(sstable);
        decrementSize(removedSize);
    }

    public void cleanupGroups() {
        groups.removeIf(t -> t.sstables.isEmpty());
    }

}