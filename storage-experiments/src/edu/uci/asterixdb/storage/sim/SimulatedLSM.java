package edu.uci.asterixdb.storage.sim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.asterixdb.storage.sim.Simulator.FlushReason;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;

class SimulatedLSM {

    protected final KeySSTable mergeRange = new KeySSTable();
    protected final KeySSTable flushRange = new KeySSTable();
    protected final KeySSTable searchKey = new KeySSTable();

    protected int memTableMinSeq = Integer.MAX_VALUE;

    protected final Simulator simulator;
    protected final Int2IntSortedMap memTableMap;
    protected final SSTable memTable;
    protected final List<PartitionedLevel> memoryLevels = new ArrayList<>();
    protected final UnpartitionedLevel unpartitionedLevel;
    protected final List<PartitionedLevel> diskLevels = new ArrayList<>();
    protected final LSMConfig config;

    protected int minSeq = 0;

    public SimulatedLSM(Simulator simulator, LSMConfig config) {
        this.simulator = simulator;
        this.config = config;
        unpartitionedLevel = new UnpartitionedLevel(-1);
        this.memTableMap = new Int2IntAVLTreeMap();
        this.memTable = new MemorySSTable(config.memConfig.activeSize);
    }

    public void resetStats() {
        for (PartitionedLevel level : memoryLevels) {
            level.resetStats();
        }
        unpartitionedLevel.resetStats();
        for (PartitionedLevel level : diskLevels) {
            level.resetStats();
        }
    }

    public void deserialize(DataInput input) throws IOException {
        int card = input.readInt();
        if (config.cardinality != card) {
            throw new IllegalStateException(
                    "Mismatched cardinalities. Expected " + config.cardinality + " actual " + card);
        }
        unpartitionedLevel.deserialize(input, simulator, this);
        int numLevels = input.readInt();
        diskLevels.clear();
        for (int i = 0; i < numLevels; i++) {
            PartitionedLevel level = new PartitionedLevel(i, false);
            level.deserialize(input, simulator, this);
            diskLevels.add(level);
        }
    }

    public void serialize(DataOutput output) throws IOException {
        output.writeInt(config.cardinality);
        unpartitionedLevel.serialize(output);
        output.writeInt(diskLevels.size());
        for (int i = 0; i < diskLevels.size(); i++) {
            diskLevels.get(i).serialize(output);
        }
    }

    public void write(int key, int seq) {
        if (memTableMap.isEmpty()) {
            memTableMinSeq = seq;
        }
        memTableMap.put(key, seq);

        if (memTableMap.size() >= config.memConfig.activeSize) {
            memoryFlush();
        }
    }

    protected void read(int key) {
        // check memory
        if (memTableMap.containsKey(key)) {
            return;
        }
        searchKey.reset(-1);
        searchKey.resetKey(key);
        // check memory levels
        for (int i = 0; i < memoryLevels.size(); i++) {
            PartitionedLevel level = memoryLevels.get(i);
            if (Utils.contains(level.sstables, searchKey)) {
                return;
            }
        }

        for (int i = 0; i < unpartitionedLevel.groups.size(); i++) {
            SSTableGroup group = unpartitionedLevel.groups.get(i);
            if (Utils.contains(group.sstables, searchKey)) {
                return;
            }
        }

        for (int i = 0; i < diskLevels.size(); i++) {
            PartitionedLevel level = diskLevels.get(i);
            if (Utils.contains(level.sstables, searchKey)) {
                return;
            }
        }
    }

    protected void prepareMemoryFlush() {
        memTable.reset(-1);
        for (Int2IntMap.Entry e : memTableMap.int2IntEntrySet()) {
            memTable.add(e.getIntKey(), e.getIntValue(), false, false);
        }
        memTableMap.clear();
        memTableMinSeq = Integer.MAX_VALUE;

        increaseMemorySize(memTable.getSize());
    }

    protected void memoryFlush() {
        prepareMemoryFlush();

        boolean addLevel = memoryLevels.isEmpty() || memoryLevels.get(0).getSize() > memTable.getSize();
        Set<SSTable> overlappingSSTables = addLevel ? Collections.emptySet()
                : Utils.findOverlappingSSTables(memTable, memoryLevels.get(0).sstables);

        MergeIterator iterator = new PartitionedIterator(memTable, overlappingSSTables);

        List<SSTable> newSSTables = buildSSTables(iterator, true, -1);

        simulator.freeSSTables(overlappingSSTables);
        decreaseMemorySize(memTable.getSize());
        if (addLevel) {
            Utils.addLevel(newSSTables, memoryLevels, true);
            increaseMemorySize(memoryLevels.get(0).getSize());
        } else {
            decreaseMemorySize(memoryLevels.get(0).getSize());
            memoryLevels.get(0).replace(overlappingSSTables, newSSTables);
            increaseMemorySize(memoryLevels.get(0).getSize());
        }
        // do memory merge
        scheduleMerge(null, memoryLevels, config.memConfig.sizeRatio);
        memTable.reset(-1);
    }

    protected void increaseMemorySize(long size) {
        simulator.usedWriteMem += size;
        simulator.stats.updateMaxWriteMem(simulator.usedWriteMem);
    }

    protected void decreaseMemorySize(long size) {
        simulator.usedWriteMem -= size;
    }

    protected void updateMinSeq() {
        minSeq = Math.min(simulator.nextSeq, memTableMinSeq);
        for (PartitionedLevel level : memoryLevels) {
            for (SSTable sstable : level.sstables) {
                minSeq = Math.min(minSeq, ((MemorySSTable) sstable).minSeq);
            }
        }
    }

    protected void scheduleMerge(UnpartitionedLevel unpartitionedLevel, List<PartitionedLevel> levels, int sizeRatio) {
        int numLevels = levels.size();
        while (true) {
            double maxScore = 0;
            int maxLevel = 0;
            GroupSelection groupSelection = selectGroupToMerge(unpartitionedLevel, levels);
            if (groupSelection != null) {
                maxLevel = -1;
                maxScore = 1.0;
            }

            for (int i = 0; i < numLevels - 1; i++) {
                double score = (double) levels.get(i).getSize() / Utils.getLevelCapacity(levels, i, sizeRatio);
                if (score > maxScore) {
                    maxScore = score;
                    maxLevel = i;
                }
            }
            if (maxScore >= 1.0) {
                if (maxLevel == -1) {
                    doUnpartitionedMerge(unpartitionedLevel, levels, groupSelection);
                } else {
                    doPartitionedMerge(levels, maxLevel);
                }
            } else {
                break;
            }
        }
    }

    protected void doPartitionedMerge(List<PartitionedLevel> levels, int level) {
        PartitionedLevel current = levels.get(level);
        PartitionedLevel next = levels.get(level + 1);
        boolean isMemory = (levels == memoryLevels);
        SSTableSelector selector = isMemory ? HybridSelector.INSTANCE : GreedySelector.INSTANCE;
        Pair<SSTable, Set<SSTable>> pair = selector.selectMerge(this, current, next.sstables);

        if (pair.getRight().isEmpty()) {
            // special case
            current.remove(pair.getLeft());
            next.add(pair.getLeft());
        } else {
            PartitionedIterator iterator = new PartitionedIterator(pair.getLeft(), pair.getRight());

            current.mergedKeys += pair.getLeft().getSize();
            current.overlapingKeys += Utils.getTotalSize(pair.getRight());
            List<SSTable> newSSTables = buildSSTables(iterator, isMemory, next.level);

            int newKeys = Utils.getTotalSize(newSSTables);
            current.resultingKeys += newKeys;

            // must free before modification; otherwise, the content of the subList will be modified
            simulator.freeSSTable(pair.getLeft());
            simulator.freeSSTables(pair.getRight());

            current.remove(pair.getLeft());
            int increments = (int) next.replace(pair.getRight(), newSSTables);

            if (isMemory) {
                simulator.stats.memoryMergeKeys += newKeys;
                int reduced = pair.getLeft().getSize() - increments;
                assert reduced >= 0;
                decreaseMemorySize(reduced);
                updateMinSeq();
            } else {
                simulator.stats.diskMergeKeys += newKeys;
            }
        }
    }

    protected String printLevels() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < memoryLevels.size(); i++) {
            sb.append("Memory ");
            sb.append(i);
            sb.append(":");
            printLevel(sb, memoryLevels.get(i));
            sb.append("\n");
        }
        sb.append("Unpartitioned: ");
        sb.append(unpartitionedLevel.groups.size());
        sb.append("/");
        sb.append(unpartitionedLevel.getSize());
        sb.append("\n");

        for (int i = 0; i < diskLevels.size(); i++) {
            sb.append("Disk ");
            sb.append(i);
            sb.append(":");
            printLevel(sb, diskLevels.get(i));
            sb.append("\n");
        }
        return sb.toString();
    }

    private void printLevel(StringBuilder sb, PartitionedLevel level) {
        sb.append(level.sstables.size());
        sb.append("/");
        sb.append(level.getSize());
        sb.append("/");
        sb.append(level.mergedKeys);
        sb.append("/");
        sb.append(Utils.formatDivision(level.overlapingKeys, level.mergedKeys));
        sb.append("/");
        sb.append(Utils.formatDivision(level.resultingKeys, level.mergedKeys));
    }

    protected List<SSTable> buildSSTables(MergeIterator iterator, boolean isMemory, int level) {
        KeyEntry entry = new KeyEntry();
        SSTable newSSTable = null;
        List<SSTable> newSSTables = new ArrayList<>();
        boolean isFlush = level == 0;
        while (iterator.hasNext()) {
            iterator.getNext(entry);
            if (newSSTable == null) {
                newSSTable = simulator.getFreeSSTable(this, isMemory, level);
                newSSTables.add(newSSTable);
            }
            newSSTable.add(entry.key, entry.seq, entry.cached, false);
            if (newSSTable.isFull()) {
                newSSTable.endBulkLoad(isFlush);
                newSSTable = null;
            }
        }
        return newSSTables;
    }

    public void diskFlush(FlushReason request) {
        List<List<SSTable>> sstables = null;
        int startLevel = -1;
        boolean flushingMemTable = false;
        if (memoryLevels.isEmpty()) {
            flushingMemTable = true;
            sstables = Collections.singletonList(Collections.singletonList(memTable));
            if (memTable.getSize() == 0) {
                prepareMemoryFlush();
            }
        } else {
            if (request == FlushReason.MEMORY) {
                simulator.stats.numMemoryFlushes++;
                startLevel = memoryLevels.size() - 1;
                SSTable sstable = OldestMinLSNSelector.INSTANCE
                        .selectMerge(this, memoryLevels.get(startLevel), Simulator.Empty_TreeSet).getLeft();
                sstables = Collections.singletonList(Collections.singletonList(sstable));

            } else {
                SSTable oldestSSTable = null;
                for (int i = 0; i < memoryLevels.size(); i++) {
                    PartitionedLevel level = memoryLevels.get(i);
                    for (SSTable sstable : level.sstables) {
                        if (oldestSSTable == null || sstable.getMinSeq() < oldestSSTable.getMinSeq()) {
                            oldestSSTable = sstable;
                            startLevel = i;
                        }
                    }
                }
                assert oldestSSTable.getMinSeq() == minSeq;
                sstables = new ArrayList<>();
                sstables.add(Collections.singletonList(oldestSSTable));
                flushRange.resetRange();
                flushRange.updateRange(oldestSSTable);
                simulator.stats.numLogFlushes++;
                for (int i = startLevel + 1; i < memoryLevels.size(); i++) {
                    NavigableSet<SSTable> overlap =
                            Utils.findOverlappingSSTables(flushRange, memoryLevels.get(i).sstables);
                    simulator.stats.numLogFlushes += overlap.size();
                    sstables.add(new ArrayList<>(overlap));
                    if (!overlap.isEmpty()) {
                        flushRange.updateRange(overlap.first());
                        flushRange.updateRange(overlap.last());
                    }
                }
            }
        }

        MergeIterator iterator = new DiskFlushIterator(sstables);

        List<SSTable> newSSTables = buildSSTables(iterator, false, unpartitionedLevel.level);

        long totalSize = addFlushedSSTable(newSSTables);
        simulator.stats.diskMergeKeys += totalSize;

        int flushSize = 0;
        for (List<SSTable> list : sstables) {
            flushSize += Utils.getTotalSize(list);
        }
        decreaseMemorySize(flushSize);
        simulator.stats.totalFlushedSize += flushSize;
        if (flushingMemTable) {
            minSeq = simulator.nextSeq - 1;
            memTable.reset(-1);
        } else {
            for (List<SSTable> list : sstables) {
                simulator.freeSSTables(list);
            }
            boolean cleanup = false;
            for (int i = 0; i < sstables.size(); i++) {
                PartitionedLevel level = memoryLevels.get(i + startLevel);
                for (SSTable unit : sstables.get(i)) {
                    level.remove(unit);
                }
                if (level.sstables.isEmpty()) {
                    cleanup = true;
                }
            }
            if (cleanup) {
                memoryLevels.removeIf(t -> t.sstables.isEmpty());
            }
        }
        updateMinSeq();

        scheduleMerge(unpartitionedLevel, diskLevels, config.diskConfig.sizeRatio);
    }

    private int addFlushedSSTable(List<SSTable> sstables) {
        int totalSize = 0;
        for (SSTable sstable : sstables) {
            SSTableGroup targetGroup = null;

            if (unpartitionedLevel.groups.isEmpty() || unpartitionedLevel.groups.get(0).sstables.contains(sstable)) {
                targetGroup = new SSTableGroup(Collections.emptyList());
                unpartitionedLevel.groups.add(0, targetGroup);
            } else {
                targetGroup = unpartitionedLevel.groups.get(0);
                // try to insert as much as possible
                for (int i = 1; i < unpartitionedLevel.groups.size(); i++) {
                    SSTableGroup group = unpartitionedLevel.groups.get(i);
                    if (!group.sstables.contains(sstable)) {
                        targetGroup = group;
                    } else {
                        break;
                    }
                }
            }
            targetGroup.add(sstable);
            totalSize += sstable.getSize();
            unpartitionedLevel.addSize(sstable.getSize());
        }
        return totalSize;

    }

    private GroupSelection selectGroupToMerge(UnpartitionedLevel unpartitionedLevel,
            List<PartitionedLevel> partitionedLevels) {
        if (unpartitionedLevel == null
                || unpartitionedLevel.groups.size() <= config.diskConfig.maxUnpartitionedSSTables) {
            return null;
        }

        long baseSize = config.memConfig.activeSize;

        boolean addLevel = partitionedLevels.isEmpty() || Utils.getLevelCapacity(partitionedLevels, 0,
                config.diskConfig.sizeRatio) > baseSize * config.diskConfig.sizeRatio;

        int groupIndex = unpartitionedLevel.groups.size() - 1;
        return new GroupSelection(groupIndex, -1, -1, addLevel);
    }

    private void doUnpartitionedMerge(UnpartitionedLevel unpartitionedLevel, List<PartitionedLevel> levels,
            GroupSelection groupSelection) {
        SSTableGroup selectedGroup = selectMinGroupToMerge(unpartitionedLevel);
        SSTable selectedSSTable = selectedGroup.sstables.first();
        unpartitionedLevel.remove(selectedGroup, selectedSSTable);

        List<SSTable> mergingSSTables = new ArrayList<>();
        mergingSSTables.add(selectedSSTable);
        mergeRange.resetRange();
        mergeRange.updateRange(selectedSSTable);

        int numGroups = 0;
        for (int i = 1; i < unpartitionedLevel.groups.size(); i++) {
            SSTableGroup group = unpartitionedLevel.groups.get(i);
            if (group == selectedGroup) {
                numGroups++;
            } else {
                NavigableSet<SSTable> set = Utils.findOverlappingSSTables(selectedSSTable, group.sstables);
                if (!set.isEmpty()) {
                    mergeRange.updateRange(set.first());
                    mergeRange.updateRange(set.last());
                    mergingSSTables.addAll(set);
                    unpartitionedLevel.remove(group, set);
                    numGroups++;
                }
            }
        }

        simulator.stats.totalUnpartitionedMerges++;
        simulator.stats.totalUnpartitionedMergeGroups += numGroups;

        unpartitionedLevel.cleanupGroups();

        TreeSet<SSTable> partitionedSSTables =
                groupSelection.addLevel ? Simulator.Empty_TreeSet : levels.get(0).sstables;
        Set<SSTable> nextSSTables = Utils.findOverlappingSSTables(mergeRange, partitionedSSTables);
        List<SSTable> newSSTables = null;
        int newKeys = 0;
        if (nextSSTables.isEmpty() && mergingSSTables.size() == 1) {
            // simply push
            newSSTables = Collections.singletonList(mergingSSTables.get(0));
        } else {
            MergeIterator unpartitionedIterator = new UnpartitionedIterator(mergingSSTables, nextSSTables);
            newSSTables = buildSSTables(unpartitionedIterator, false, 0);
            simulator.freeSSTables(mergingSSTables);
            simulator.freeSSTables(nextSSTables);

            newKeys = Utils.getTotalSize(newSSTables);
            simulator.stats.diskMergeKeys += newKeys;
        }

        if (groupSelection.addLevel) {
            Utils.addLevel(newSSTables, levels, false);
        } else {
            levels.get(0).replace(nextSSTables, newSSTables);
        }
    }

    private SSTableGroup selectMinGroupToMerge(UnpartitionedLevel level) {
        if (level.groups.size() == 1) {
            return level.groups.get(0);
        }
        SSTableGroup group = null;
        for (int i = 1; i < level.groups.size(); i++) {
            if (group == null || level.groups.get(i).sstables.size() < group.sstables.size()) {
                group = level.groups.get(i);
            }
        }
        return group;
    }

}