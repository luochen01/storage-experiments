package edu.uci.asterixdb.storage.sim;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

interface SSTableSelector {
    public Pair<StorageUnit, Set<StorageUnit>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<StorageUnit> nextLevel);
}

class HybridSelector implements SSTableSelector {
    public static final HybridSelector INSTANCE = new HybridSelector();

    private HybridSelector() {

    }

    @Override
    public Pair<StorageUnit, Set<StorageUnit>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<StorageUnit> nextLevel) {
        if (!currentLevel.inMemory) {
            throw new IllegalStateException();
        }
        if (currentLevel.level == sim.memoryLevels.size() - 1) {
            return OldestMinLSNSelector.INSTANCE.selectMerge(sim, currentLevel, nextLevel);
        } else {
            return RoundRobinSelector.INSTANCE.selectMerge(sim, currentLevel, nextLevel);
        }
    }

}

class RoundRobinSelector implements SSTableSelector {
    public static final RoundRobinSelector INSTANCE = new RoundRobinSelector();

    private RoundRobinSelector() {

    }

    @Override
    public Pair<StorageUnit, Set<StorageUnit>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<StorageUnit> nextLevel) {
        StorageUnit sstable = Utils.getNextMergeSSTable(currentLevel.sstables, currentLevel.lastKey);
        currentLevel.lastKey.resetKey(sstable.max());
        Set<StorageUnit> overlappingSSTables = Utils.findOverlappingSSTables(sstable, nextLevel);
        return Pair.of(sstable, overlappingSSTables);
    }
}

class OldestMinLSNSelector implements SSTableSelector {
    public static final OldestMinLSNSelector INSTANCE = new OldestMinLSNSelector();

    private OldestMinLSNSelector() {
    }

    @Override
    public Pair<StorageUnit, Set<StorageUnit>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<StorageUnit> nextLevel) {
        SSTable minSSTable = (SSTable) selectOldestSSTable(currentLevel);
        if (minSSTable == null) {
            return null;
        } else {
            Set<StorageUnit> overlappingSSTables = Utils.findOverlappingSSTables(minSSTable, nextLevel);
            return Pair.of(minSSTable, overlappingSSTables);
        }
    }

    private StorageUnit selectOldestSSTable(PartitionedLevel level) {
        StorageUnit minSSTable = null;
        long minSeq = Long.MAX_VALUE;
        for (StorageUnit unit : level.sstables) {
            SSTable sstable = (SSTable) unit;
            if (sstable.minSeq < minSeq) {
                minSeq = sstable.minSeq;
                minSSTable = sstable;
            }
        }
        return minSSTable;
    }
}

class GreedySelector implements SSTableSelector {

    public static final GreedySelector INSTANCE = new GreedySelector();

    private GreedySelector() {
    }

    @Override
    public Pair<StorageUnit, Set<StorageUnit>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<StorageUnit> nextLevel) {
        StorageUnit selectedUnit = null;
        int selectedSize = Integer.MAX_VALUE;

        for (StorageUnit unit : currentLevel.sstables) {
            Set<StorageUnit> overlappingUnits = Utils.findOverlappingSSTables(unit, nextLevel);

            int size = Utils.getTotalSize(overlappingUnits);
            if (size < selectedSize) {
                selectedSize = size;
                selectedUnit = unit;
            }
        }
        Set<StorageUnit> selectedOverlappingUnits = Utils.findOverlappingSSTables(selectedUnit, nextLevel);
        return Pair.of(selectedUnit, selectedOverlappingUnits);
    }
}