package edu.uci.asterixdb.storage.sim;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

interface SSTableSelector {
    public Pair<SSTable, Set<SSTable>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel);
}

class HybridSelector implements SSTableSelector {
    public static final HybridSelector INSTANCE = new HybridSelector();

    private HybridSelector() {

    }

    @Override
    public Pair<SSTable, Set<SSTable>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel) {
        if (!currentLevel.inMemory) {
            throw new IllegalStateException();
        }
        if (currentLevel.level == sim.memoryLevels.size() - 1) {
            return OldestMinLSNSelector.INSTANCE.selectMerge(sim, currentLevel, nextLevel);
        } else {
            return GreedySelector.INSTANCE.selectMerge(sim, currentLevel, nextLevel);
        }
    }

}

class RoundRobinSelector implements SSTableSelector {
    public static final RoundRobinSelector INSTANCE = new RoundRobinSelector();

    private RoundRobinSelector() {

    }

    @Override
    public Pair<SSTable, Set<SSTable>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel) {
        SSTable sstable = Utils.getNextMergeSSTable(currentLevel.sstables, currentLevel.lastKey);
        currentLevel.lastKey.resetKey(sstable.max());
        Set<SSTable> overlappingSSTables = Utils.findOverlappingSSTables(sstable, nextLevel);
        return Pair.of(sstable, overlappingSSTables);
    }
}

class OldestMinLSNSelector implements SSTableSelector {
    public static final OldestMinLSNSelector INSTANCE = new OldestMinLSNSelector();

    private OldestMinLSNSelector() {
    }

    @Override
    public Pair<SSTable, Set<SSTable>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel) {
        SSTable minSSTable = selectOldestSSTable(currentLevel);
        if (minSSTable == null) {
            return null;
        } else {
            Set<SSTable> overlappingSSTables = Utils.findOverlappingSSTables(minSSTable, nextLevel);
            return Pair.of(minSSTable, overlappingSSTables);
        }
    }

    private SSTable selectOldestSSTable(PartitionedLevel level) {
        SSTable minSSTable = null;
        long minSeq = Long.MAX_VALUE;
        for (SSTable sstable : level.sstables) {
            MemorySSTable memSSTable = (MemorySSTable) sstable;
            if (memSSTable.minSeq < minSeq) {
                minSeq = memSSTable.minSeq;
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
    public Pair<SSTable, Set<SSTable>> selectMerge(LSMSimulator sim, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel) {
        SSTable selectedUnit = null;
        double selectedRatio = 0;

        for (SSTable unit : currentLevel.sstables) {
            Set<SSTable> overlappingUnits = Utils.findOverlappingSSTables(unit, nextLevel);

            double ratio = (double) Utils.getTotalSize(overlappingUnits) / unit.getSize();
            if (selectedUnit == null || ratio < selectedRatio) {
                selectedRatio = ratio;
                selectedUnit = unit;
            }
        }
        Set<SSTable> selectedOverlappingUnits = Utils.findOverlappingSSTables(selectedUnit, nextLevel);
        return Pair.of(selectedUnit, selectedOverlappingUnits);
    }
}