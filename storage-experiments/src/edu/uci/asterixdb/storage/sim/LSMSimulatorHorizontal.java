package edu.uci.asterixdb.storage.sim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class LSMSimulatorHorizontal extends LSMSimulator {

    protected final SSTable mergeRange = new SSTable(2, false);

    public LSMSimulatorHorizontal(KeyGenerator keyGen, Config config) {
        super(keyGen, config);
    }

    @Override
    protected void diskFlush() {
        SSTable sstable = memoryLevels.isEmpty() ? memTable
                : (SSTable) config.memSSTableSelector
                        .selectMerge(this, memoryLevels.get(memoryLevels.size() - 1), Empty_TreeSet).getLeft();

        if (sstable == memTable && memTable.getSize() == 0) {
            prepareMemoryFlush();
            assert memTable.getSize() > 0;
        }

        MergeIterator iterator = new DiskFlushIterator(sstable);

        List<StorageUnit> newSSTables = buildSSTables(iterator, false);

        long totalSize = addFlushedSSTable(newSSTables);
        diskMergeKeys += totalSize;

        decreaseMemorySize(sstable.getSize());
        if (sstable != memTable) {
            freeSSTable(sstable);
            PartitionedLevel lastLevel = memoryLevels.get(memoryLevels.size() - 1);
            lastLevel.remove(sstable);
            if (lastLevel.sstables.isEmpty()) {
                assert (lastLevel.getSize() == 0);
                memoryLevels.remove(memoryLevels.size() - 1);
            }

            if (VERBOSE) {
                System.out.println(String.format("Disk flush mlevel %d sstable %s with %d keys", lastLevel.level,
                        sstable.toString(), sstable.getSize()));
            }

        } else {
            minSeq = nextSeq - 1;
            memTable.reset();
        }
        updateMinSeq();

        scheduleMerge(unpartitionedLevel, diskLevels, config.diskConfig.sizeRatio);
    }

    private int addFlushedSSTable(List<StorageUnit> sstables) {
        int totalSize = 0;
        for (StorageUnit sstable : sstables) {
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

    @Override
    protected GroupSelection selectGroupToMerge(UnpartitionedLevel unpartitionedLevel,
            List<PartitionedLevel> partitionedLevels) {
        if (unpartitionedLevel == null
                || unpartitionedLevel.groups.size() <= config.diskConfig.maxUnpartitionedSSTables) {
            return null;
        }

        long baseSize = config.diskConfig.useMemorySizeForAddLevel && !memoryLevels.isEmpty()
                ? memoryLevels.get(memoryLevels.size() - 1).getSize()
                : config.diskConfig.sstableSize;

        boolean addLevel = partitionedLevels.isEmpty() || Utils.getLevelCapacity(partitionedLevels, 0,
                config.diskConfig.sizeRatio) > baseSize * config.diskConfig.sizeRatio;

        int groupIndex = unpartitionedLevel.groups.size() - 1;
        return new GroupSelection(groupIndex, -1, -1, addLevel);
    }

    @Override
    protected void doUnpartitionedMerge(UnpartitionedLevel unpartitionedLevel, List<PartitionedLevel> levels,
            GroupSelection groupSelection) {
        SSTableGroup selectedGroup = selectMinGroupToMerge(unpartitionedLevel);
        SSTable selectedSSTable = (SSTable) selectedGroup.sstables.first();
        unpartitionedLevel.remove(selectedGroup, selectedSSTable);

        List<StorageUnit> mergingSSTables = new ArrayList<>();
        mergingSSTables.add(selectedSSTable);
        mergeRange.resetRange();
        mergeRange.updateRange(selectedSSTable);

        int numGroups = 0;
        for (int i = 1; i < unpartitionedLevel.groups.size(); i++) {
            SSTableGroup group = unpartitionedLevel.groups.get(i);
            if (group == selectedGroup) {
                numGroups++;
            } else {
                TreeSet<StorageUnit> set = Utils.findOverlappingSSTables(selectedSSTable, group.sstables);
                if (!set.isEmpty()) {
                    mergeRange.updateRange(set.first());
                    mergeRange.updateRange(set.last());
                    mergingSSTables.addAll(set);
                    unpartitionedLevel.remove(group, set);
                    numGroups++;
                }
            }
        }

        totalUnpartitionedMerges++;
        totalUnpartitionedMergeGroups += numGroups;
        if (VERBOSE) {
            System.out.println(String.format("Merged %d SSTables with %d groups at the unpartitioned level at once",
                    mergingSSTables.size(), numGroups));
        }

        unpartitionedLevel.cleanupGroups();

        TreeSet<StorageUnit> partitionedSSTables = groupSelection.addLevel ? Empty_TreeSet : levels.get(0).sstables;
        Set<StorageUnit> nextSSTables = Utils.findOverlappingSSTables(mergeRange, partitionedSSTables);
        List<StorageUnit> newSSTables = null;
        int newKeys = 0;
        if (nextSSTables.isEmpty() && mergingSSTables.size() == 1) {
            // simply push
            newSSTables = Collections.singletonList(mergingSSTables.get(0));
        } else {
            MergeIterator unpartitionedIterator = new UnpartitionedIterator(mergingSSTables, nextSSTables);
            newSSTables = buildSSTables(unpartitionedIterator, false);
            freeSSTables(mergingSSTables);
            freeSSTables(nextSSTables);

            newKeys = Utils.getTotalSize(newSSTables);
            diskMergeKeys += newKeys;
        }

        if (!loading && VERBOSE) {
            System.out.println(String.format("Unpartitinoed merge %d sstables %s overlapping with %d/%d",
                    mergingSSTables.size(), mergingSSTables.toString(), newSSTables.size(), newKeys));
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
