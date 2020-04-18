package edu.uci.asterixdb.storage.sim.lsm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

abstract class Level {
    final boolean inMemory;
    int level;
    private int size = 0;

    long mergedKeys = 0;
    long overlapingKeys = 0;
    long resultingKeys = 0;

    public Level(int level, boolean inMemory) {
        this.level = level;
        this.inMemory = inMemory;
    }

    public int getSize() {
        return size;
    }

    public void serialize(DataOutput output) throws IOException {
        output.writeInt(level);
        output.writeInt(size);
    }

    public void deserialize(DataInput input, Simulator sim, SimulatedLSM lsm) throws IOException {
        level = input.readInt();
        size = input.readInt();
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (inMemory ? 1231 : 1237);
        result = prime * result + level;
        result = prime * result + (size ^ (size >>> 32));
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
        Level other = (Level) obj;
        if (inMemory != other.inMemory)
            return false;
        if (level != other.level)
            return false;
        if (size != other.size)
            return false;
        return true;
    }

}

class PartitionedLevel extends Level {

    final TreeSet<SSTable> sstables = new TreeSet<>();
    int lastKey = -1;

    public PartitionedLevel(int level, boolean isMemory) {
        super(level, isMemory);
    }

    public void remove(SSTable sstable) {
        if (!sstables.remove(sstable)) {
            throw new IllegalStateException("removing non-existing sstable " + sstable);
        }
        decrementSize(sstable.getSize());
    }

    public void add(SSTable sstable) {
        if (!sstables.add(sstable)) {
            throw new IllegalStateException("adding duplicated sstable " + sstable);
        }
        addSize(sstable.getSize());
    }

    public long replace(Set<SSTable> oldSet, List<SSTable> newList) {
        long oldSize = getSize();
        remove(oldSet);
        newList.forEach(t -> add(t));
        return getSize() - oldSize;
    }

    private void remove(Set<SSTable> oldSet) {
        int totalSize = 0;
        for (SSTable unit : oldSet) {
            totalSize += unit.getSize();
        }
        decrementSize(totalSize);

        int oldSize = sstables.size();
        int oldSetSize = oldSet.size();
        oldSet.clear();
        int newSize = sstables.size();
        assert newSize == oldSize - oldSetSize;

    }

    @Override
    public void serialize(DataOutput output) throws IOException {
        super.serialize(output);
        output.writeInt(sstables.size());
        for (SSTable sstable : sstables) {
            sstable.serialize(output);
        }
    }

    @Override
    public void deserialize(DataInput input, Simulator sim, SimulatedLSM lsm) throws IOException {
        super.deserialize(input, sim, lsm);
        int num = input.readInt();
        sstables.clear();
        for (int i = 0; i < num; i++) {
            SSTable sstable = sim.getFreeSSTable(lsm, false, level);
            sstable.deserialize(input, level);
            sstables.add(sstable);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((sstables == null) ? 0 : sstables.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionedLevel other = (PartitionedLevel) obj;
        if (sstables == null) {
            if (other.sstables != null)
                return false;
        } else if (!sstables.equals(other.sstables))
            return false;
        return true;
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

    @Override
    public void serialize(DataOutput output) throws IOException {
        super.serialize(output);
        output.writeInt(groups.size());
        for (SSTableGroup group : groups) {
            group.serialize(output);
        }
    }

    @Override
    public void deserialize(DataInput input, Simulator sim, SimulatedLSM lsm) throws IOException {
        super.deserialize(input, sim, lsm);
        int num = input.readInt();
        groups.clear();
        for (int i = 0; i < num; i++) {
            SSTableGroup group = new SSTableGroup(Collections.emptyList());
            group.deserialize(input, level, sim, lsm);
            groups.add(group);
        }
    }

    public void addGroups(List<SSTableGroup> newGroups) {
        newGroups.forEach(t -> add(t));
    }

    public void clearGroup(SSTableGroup group) {
        decrementSize(group.getSize());
        group.clear();
    }

    public void remove(SSTableGroup group, Set<SSTable> sstables) {
        int removedSize = group.remove(sstables);
        decrementSize(removedSize);
    }

    public void remove(SSTableGroup group, SSTable sstable) {
        int removedSize = group.remove(sstable);
        decrementSize(removedSize);
    }

    public void cleanupGroups() {
        groups.removeIf(t -> t.sstables.isEmpty());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((groups == null) ? 0 : groups.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        UnpartitionedLevel other = (UnpartitionedLevel) obj;
        if (groups == null) {
            if (other.groups != null)
                return false;
        } else if (!groups.equals(other.groups))
            return false;
        return true;
    }

}