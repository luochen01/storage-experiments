package edu.uci.asterixdb.storage.sim.lsm;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

interface SSTableSelector {
    public Pair<SSTable, Set<SSTable>> selectMerge(SimulatedLSM lsm, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel);
}

class HybridSelector implements SSTableSelector {
    public static final HybridSelector INSTANCE = new HybridSelector();

    private HybridSelector() {

    }

    @Override
    public Pair<SSTable, Set<SSTable>> selectMerge(SimulatedLSM lsm, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel) {
        if (!currentLevel.inMemory) {
            throw new IllegalStateException();
        }
        if (currentLevel.level == lsm.memoryLevels.size() - 1) {
            return OldestMinLSNSelector.INSTANCE.selectMerge(lsm, currentLevel, nextLevel);
        } else {
            return GreedySelector.INSTANCE.selectMerge(lsm, currentLevel, nextLevel);
        }
    }
}

class RoundRobinSelector implements SSTableSelector {

    public static final RoundRobinSelector INSTANCE = new RoundRobinSelector();

    private final ThreadLocal<KeySSTable> localKey = new ThreadLocal<>();

    @Override
    public Pair<SSTable, Set<SSTable>> selectMerge(SimulatedLSM lsm, PartitionedLevel currentLevel,
            TreeSet<SSTable> nextLevel) {
        KeySSTable lastKey = setKey(currentLevel.lastKey);
        SSTable sstable = currentLevel.sstables.higher(lastKey);
        if (sstable == null) {
            sstable = currentLevel.sstables.first();
        }
        Set<SSTable> overlappingSSTables = Utils.findOverlappingSSTables(sstable, nextLevel);
        return Pair.of(sstable, overlappingSSTables);
    }

    private KeySSTable setKey(int lastKey) {
        KeySSTable key = localKey.get();
        if (key == null) {
            key = new KeySSTable();
            localKey.set(key);
        }
        key.resetKey(lastKey);
        return key;
    }
}

class OldestMinLSNSelector implements SSTableSelector {
    public static final OldestMinLSNSelector INSTANCE = new OldestMinLSNSelector();

    private OldestMinLSNSelector() {
    }

    @Override
    public Pair<SSTable, Set<SSTable>> selectMerge(SimulatedLSM lsm, PartitionedLevel currentLevel,
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
    public Pair<SSTable, Set<SSTable>> selectMerge(SimulatedLSM lsm, PartitionedLevel currentLevel,
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