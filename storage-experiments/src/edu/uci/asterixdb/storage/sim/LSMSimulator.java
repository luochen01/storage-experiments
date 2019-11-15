package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

class MemoryConfig {
    final int activeSize;
    int totalMemSize;
    final int sstableSize;
    final int sizeRatio;
    final boolean enableMemoryMerge;

    public MemoryConfig(int activeSize, int totalMemSize, int sstableSize, int sizeRatio, boolean enableMemoryMerge) {
        super();
        this.activeSize = activeSize;
        this.totalMemSize = totalMemSize;
        this.sstableSize = sstableSize;
        this.sizeRatio = sizeRatio;
        this.enableMemoryMerge = enableMemoryMerge;
    }
}

class DiskConfig {
    final int sstableSize;
    final int sizeRatio;
    final int maxUnpartitionedSSTables;
    final boolean useMemorySizeForAddLevel;

    public DiskConfig(int sstableSize, int sizeRatio, int maxUnpartitionedSSTables, boolean useMemorySizeForAddLevel) {
        super();
        this.sstableSize = sstableSize;
        this.sizeRatio = sizeRatio;
        this.maxUnpartitionedSSTables = maxUnpartitionedSSTables;
        this.useMemorySizeForAddLevel = useMemorySizeForAddLevel;
    }
}

class TuningConfig {
    final int cacheSize;
    final int pageSize;
    final int writes;
    final int reads;

    public TuningConfig(int cacheSize, int pageSize, int writes, int reads) {
        this.cacheSize = cacheSize;
        this.pageSize = pageSize;
        this.writes = writes;
        this.reads = reads;
    }
}

class Config {
    final MemoryConfig memConfig;
    final DiskConfig diskConfig;
    final TuningConfig tuningConfig;
    final int cardinality;
    final SSTableSelector memSSTableSelector;
    final SSTableSelector diskSSTableSelector;
    final long maxLogLength;
    final long minLogLength;

    public Config(MemoryConfig memConfig, DiskConfig diskConfig, int cardinality, SSTableSelector memSSTableSelector,
            SSTableSelector diskSSTableSelector, long minLogLength, long maxLogLength) {
        this(memConfig, diskConfig, new TuningConfig(Integer.MAX_VALUE, diskConfig.sstableSize, 1, 0), cardinality,
                memSSTableSelector, diskSSTableSelector, minLogLength, maxLogLength);
    }

    public Config(MemoryConfig memConfig, DiskConfig diskConfig, TuningConfig tuningConfig, int cardinality,
            SSTableSelector memSSTableSelector, SSTableSelector diskSSTableSelector, long minLogLength,
            long maxLogLength) {
        this.memConfig = memConfig;
        this.diskConfig = diskConfig;
        this.tuningConfig = tuningConfig;
        this.cardinality = cardinality;
        this.memSSTableSelector = memSSTableSelector;
        this.diskSSTableSelector = diskSSTableSelector;
        this.minLogLength = minLogLength;
        this.maxLogLength = maxLogLength;
    }
}

class GroupSelection {
    int group;
    int fromIndex;
    int toIndex;
    boolean addLevel;

    public GroupSelection(int group, int fromIndex, int toIndex, boolean addLevel) {
        super();
        this.group = group;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
        this.addLevel = addLevel;
    }
}

class SimulationStats {
    int numDiskFlushes = 0;
    long memoryMergeKeys = 0;
    long diskMergeKeys = 0;
    int totalMemorySSTables = 0;
    int totalDiskSSTables = 0;
    int totalDiskFlushes = 0;
    int maxDiskFlushesPerLogTruncation = 0;
    int totalLogTruncations = 0;
    int totalUnpartitionedMerges = 0;
    int totalUnpartitionedMergeGroups = 0;
    int maxMemTableSize = 0;

}

public class LSMSimulator {
    public enum FlushReason {
        MEMORY,
        LOG
    }

    public static boolean ROUND_ROBIN = false;

    protected final KeySSTable mergeRange = new KeySSTable();
    protected final KeySSTable flushRange = new KeySSTable();
    protected final KeySSTable searchKey = new KeySSTable();

    public static boolean VERBOSE = false;

    public static int MEMORY_INTERVAL = 1000;

    protected static final TreeSet<SSTable> Empty_TreeSet = new TreeSet<>();

    protected final Random rand = new Random(17);
    protected final Int2IntSortedMap memTableMap;
    protected int memTableMinSeq = Integer.MAX_VALUE;
    protected final SSTable memTable;
    protected final List<PartitionedLevel> memoryLevels = new ArrayList<>();

    private int totalMemTableSize;

    protected final UnpartitionedLevel unpartitionedLevel;
    protected final List<PartitionedLevel> diskLevels = new ArrayList<>();

    protected final Config config;

    protected final KeyGenerator keyGen;
    protected final ArrayDeque<MemorySSTable> memorySSTs = new ArrayDeque<>();
    protected final ArrayDeque<DiskSSTable> diskSSTs = new ArrayDeque<>();

    public static int progress = 1000 * 1000; // 1 million
    protected boolean loading = false;

    // logging
    protected int minSeq = 0;
    protected int nextSeq = 0;

    protected PrintWriter printWriter;
    protected SimulationStats stats = new SimulationStats();

    protected final LRUCache cache;
    protected int ops;

    public LSMSimulator(KeyGenerator keyGen, Config config) {
        unpartitionedLevel = new UnpartitionedLevel(-1);
        this.keyGen = keyGen;
        this.cache = new LRUCache(config.tuningConfig.cacheSize, config.tuningConfig.pageSize);
        this.config = config;
        this.memTableMap = new Int2IntAVLTreeMap();
        this.memTable = new MemorySSTable(config.memConfig.activeSize);
    }

    public void initializeMemoryLog(File file) throws IOException {
        printWriter = new PrintWriter(file);
    }

    public void resetStats() {
        for (PartitionedLevel level : memoryLevels) {
            level.resetStats();
        }
        unpartitionedLevel.resetStats();
        for (PartitionedLevel level : diskLevels) {
            level.resetStats();
        }
        stats = new SimulationStats();
        cache.resetStats();
    }

    public void simulate(int totalOps) {
        load();
        resetStats();
        for (ops = 0; ops < totalOps;) {
            // do writes
            for (int writes = 0; writes < config.tuningConfig.writes; writes++, ops++) {
                write(keyGen.nextKey());
            }
            for (int reads = 0; reads < config.tuningConfig.reads; reads++, ops++) {
                read(keyGen.nextKey());
            }
        }

        if (printWriter != null) {
            printWriter.close();
        }
    }

    protected void load() {
        loading = true;
        IntList list = new IntArrayList(config.cardinality);
        for (int i = 0; i < config.cardinality; i++) {
            list.add(i);
        }
        IntLists.shuffle(list, rand);
        for (ops = 0; ops < config.cardinality; ops++) {
            write(list.getInt(ops));
        }

        while (totalMemTableSize > 0 || !memTableMap.isEmpty()) {
            diskFlush(FlushReason.MEMORY);
        }

        cache.resetStats();
        loading = false;
    }

    protected void write(int key) {
        if (memTableMap.isEmpty()) {
            memTableMinSeq = nextSeq;
        }
        memTableMap.put(key, nextSeq++);
        int diskFlushed = 0;
        if (config.maxLogLength > 0 && minSeq + config.maxLogLength < nextSeq) {
            while (minSeq + config.minLogLength < nextSeq) {
                diskFlush(FlushReason.LOG);
                diskFlushed++;
            }
            stats.totalLogTruncations++;
            stats.totalDiskFlushes += diskFlushed;
            stats.maxDiskFlushesPerLogTruncation = Math.max(stats.maxDiskFlushesPerLogTruncation, diskFlushed);

            if (VERBOSE && !loading && diskFlushed > 1) {
                System.out.println(stats.totalLogTruncations + ": flushed " + diskFlushed + " sstables at once");
            }
        }

        stats.numDiskFlushes += diskFlushed;

        if (memTableMap.size() >= config.memConfig.activeSize) {
            memoryFlush();
        }

        checkCounter();
    }

    protected void read(int key) {
        checkCounter();

        // check memory
        if (memTableMap.containsKey(key)) {
            return;
        }
        searchKey.reset();
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
        memTable.reset();
        for (Int2IntMap.Entry e : memTableMap.int2IntEntrySet()) {
            memTable.add(e.getIntKey(), e.getIntValue());
        }
        memTableMap.clear();
        memTableMinSeq = Integer.MAX_VALUE;

        increaseMemorySize(memTable.getSize());
    }

    protected void memoryFlush() {
        prepareMemoryFlush();

        if (memTable.getSize() >= config.memConfig.totalMemSize || !config.memConfig.enableMemoryMerge) {
            diskFlush(FlushReason.MEMORY);
        } else {
            boolean addLevel = memoryLevels.isEmpty() || memoryLevels.get(0).getSize() > memTable.getSize();
            Set<SSTable> overlappingSSTables = addLevel ? Collections.emptySet()
                    : Utils.findOverlappingSSTables(memTable, memoryLevels.get(0).sstables);

            MergeIterator iterator = new PartitionedIterator(memTable, overlappingSSTables);

            List<SSTable> newSSTables = buildSSTables(iterator, true);

            freeSSTables(overlappingSSTables);
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

            while (totalMemTableSize > config.memConfig.totalMemSize) {
                diskFlush(FlushReason.MEMORY);
            }
        }
        memTable.reset();
    }

    protected void increaseMemorySize(long size) {
        this.totalMemTableSize += size;
        stats.maxMemTableSize = Math.max(stats.maxMemTableSize, totalMemTableSize);
    }

    protected void decreaseMemorySize(long size) {
        this.totalMemTableSize -= size;
    }

    protected void updateMinSeq() {
        if (config.maxLogLength > 0) {
            minSeq = Math.min(nextSeq, memTableMinSeq);
            for (PartitionedLevel level : memoryLevels) {
                for (SSTable sstable : level.sstables) {
                    minSeq = Math.min(minSeq, ((MemorySSTable) sstable).minSeq);
                }
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
        SSTableSelector selector = isMemory ? config.memSSTableSelector : config.diskSSTableSelector;
        Pair<SSTable, Set<SSTable>> pair = selector.selectMerge(this, current, next.sstables);

        if (pair.getRight().isEmpty()) {
            // special case
            current.remove(pair.getLeft());
            next.add(pair.getLeft());

            if (VERBOSE && !loading && !isMemory) {
                System.out.println(
                        String.format("Level %d: pushing %s to the next level.", level, pair.getLeft().toString()));
            }
        } else {
            PartitionedIterator iterator = new PartitionedIterator(pair.getLeft(), pair.getRight());

            current.mergedKeys += pair.getLeft().getSize();
            current.overlapingKeys += Utils.getTotalSize(pair.getRight());
            List<SSTable> newSSTables = buildSSTables(iterator, isMemory);

            int newKeys = Utils.getTotalSize(newSSTables);
            current.resultingKeys += newKeys;

            int numNextComponents = pair.getRight().size();

            if (VERBOSE && !loading && !isMemory) {
                System.out.println(String.format(
                        "Level %d: merge %s/%d keys with %d/%d keys/SSTables at next level. Resulting keys: %d. Dupliates: %d.",
                        level, pair.getLeft().toString(), pair.getLeft().getSize(), Utils.getTotalSize(pair.getRight()),
                        numNextComponents, newKeys,
                        pair.getLeft().getSize() + Utils.getTotalSize(pair.getRight()) - newKeys));
            }

            // must free before modification; otherwise, the content of the subList has already been modifed
            freeSSTable(pair.getLeft());
            freeSSTables(pair.getRight());

            current.remove(pair.getLeft());
            int increments = (int) next.replace(pair.getRight(), newSSTables);

            if (isMemory) {
                stats.memoryMergeKeys += newKeys;
                int reduced = pair.getLeft().getSize() - increments;
                assert reduced >= 0;
                decreaseMemorySize(reduced);
                updateMinSeq();
            } else {
                stats.diskMergeKeys += newKeys;
            }
        }
    }

    protected String printWriteAmplification(long mergedKeys) {
        return String.format("%.2f", (double) mergedKeys / ops);
    }

    public String printAverageFlushesPerLogTruncation() {
        return Utils.formatDivision(stats.totalDiskFlushes, stats.totalLogTruncations);
    }

    protected List<SSTable> buildSSTables(MergeIterator iterator, boolean isMemory) {
        KeyEntry entry = new KeyEntry();
        SSTable newSSTable = null;
        List<SSTable> newSSTables = new ArrayList<>();
        while (iterator.hasNext()) {
            iterator.getNext(entry);
            if (newSSTable == null) {
                newSSTable = getFreeSSTable(isMemory);
                newSSTables.add(newSSTable);
            }
            newSSTable.add(entry.key, entry.seq);
            if (newSSTable.isFull()) {
                newSSTable.write();
                newSSTable = null;
            }
        }
        return newSSTables;
    }

    protected SSTable getFreeSSTable(boolean isMemory) {
        ArrayDeque<? extends SSTable> ssts = isMemory ? memorySSTs : diskSSTs;
        SSTable sstable = ssts.peekFirst();
        if (sstable == null) {
            if (isMemory) {
                sstable = new MemorySSTable(config.memConfig.sstableSize);
                stats.totalMemorySSTables++;
            } else {
                sstable = new DiskSSTable(config.diskConfig.sstableSize, this);
                stats.totalDiskSSTables++;
            }
        } else {
            assert sstable.isFree;
            ssts.removeFirst();
        }
        sstable.reset();
        sstable.isFree = false;
        return sstable;
    }

    protected void freeSSTable(SSTable sstable) {
        if (sstable instanceof MemorySSTable) {
            memorySSTs.add((MemorySSTable) sstable);
        } else {
            diskSSTs.add((DiskSSTable) sstable);
        }
        sstable.isFree = true;
    }

    protected void freeSSTables(Collection<SSTable> sstables) {
        sstables.forEach(sst -> freeSSTable(sst));
    }

    protected void checkCounter() {
        if (ops % progress == 0) {
            synchronized (LSMSimulator.class) {
                System.out.println("max memory table size " + stats.maxMemTableSize);
                System.out.println("current memory table size " + totalMemTableSize);
                System.out.println("total memory sstables " + stats.totalMemorySSTables);
                System.out.println("total disk sstables " + stats.totalDiskSSTables);
                System.out.println(String.format("%s: completed %d keys. memory write amp %s, disk write amp %s",
                        loading ? "Load" : "Update", ops, printWriteAmplification(stats.memoryMergeKeys),
                        printWriteAmplification(stats.diskMergeKeys)));
                System.out.println(printLevels());
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

    private void diskFlush(FlushReason request) {
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
            if (ROUND_ROBIN) {
                startLevel = memoryLevels.size() - 1;
                SSTable sstable = RoundRobinSelector.INSTANCE
                        .selectMerge(this, memoryLevels.get(startLevel), Empty_TreeSet).getLeft();
                sstables = Collections.singletonList(Collections.singletonList(sstable));
            } else {
                if (request == FlushReason.MEMORY) {
                    startLevel = memoryLevels.size() - 1;
                    SSTable sstable = OldestMinLSNSelector.INSTANCE
                            .selectMerge(this, memoryLevels.get(startLevel), Empty_TreeSet).getLeft();
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
                    for (int i = startLevel + 1; i < memoryLevels.size(); i++) {
                        NavigableSet<SSTable> overlap =
                                Utils.findOverlappingSSTables(flushRange, memoryLevels.get(i).sstables);
                        sstables.add(new ArrayList<>(overlap));
                        if (!overlap.isEmpty()) {
                            flushRange.updateRange(overlap.first());
                            flushRange.updateRange(overlap.last());
                        }
                    }
                }
            }

        }

        MergeIterator iterator = new DiskFlushIterator(sstables);

        List<SSTable> newSSTables = buildSSTables(iterator, false);

        long totalSize = addFlushedSSTable(newSSTables);
        stats.diskMergeKeys += totalSize;

        int flushSize = 0;
        for (List<SSTable> list : sstables) {
            flushSize += Utils.getTotalSize(list);
        }
        decreaseMemorySize(flushSize);
        if (flushingMemTable) {
            minSeq = nextSeq - 1;
            memTable.reset();
        } else {
            for (List<SSTable> list : sstables) {
                freeSSTables(list);
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

        long baseSize = config.diskConfig.useMemorySizeForAddLevel && !memoryLevels.isEmpty()
                ? memoryLevels.get(memoryLevels.size() - 1).getSize()
                : config.diskConfig.sstableSize;

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

        stats.totalUnpartitionedMerges++;
        stats.totalUnpartitionedMergeGroups += numGroups;
        if (VERBOSE) {
            System.out.println(String.format("Merged %d SSTables with %d groups at the unpartitioned level at once",
                    mergingSSTables.size(), numGroups));
        }

        unpartitionedLevel.cleanupGroups();

        TreeSet<SSTable> partitionedSSTables = groupSelection.addLevel ? Empty_TreeSet : levels.get(0).sstables;
        Set<SSTable> nextSSTables = Utils.findOverlappingSSTables(mergeRange, partitionedSSTables);
        List<SSTable> newSSTables = null;
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
            stats.diskMergeKeys += newKeys;
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
