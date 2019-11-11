package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

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

class Config {
    final MemoryConfig memConfig;
    final DiskConfig diskConfig;
    final int cardinality;
    final SSTableSelector memSSTableSelector;
    final SSTableSelector diskSSTableSelector;
    final long maxLogLength;
    final long minLogLength;

    public Config(MemoryConfig memConfig, DiskConfig diskConfig, int cardinality, SSTableSelector memSSTableSelector,
            SSTableSelector diskSSTableSelector, long minLogLength, long maxLogLength) {
        this.memConfig = memConfig;
        this.diskConfig = diskConfig;
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

public abstract class LSMSimulator {
    public enum FlushReason {
        MEMORY,
        LOG
    }

    public static boolean VERBOSE = false;

    public static int MEMORY_INTERVAL = 1000;

    protected static final TreeSet<StorageUnit> Empty_TreeSet = new TreeSet<>();

    protected final Random rand = new Random(17);
    protected final TreeMap<Long, Long> memTableMap;
    protected long memTableMinSeq = Long.MAX_VALUE;
    protected final SSTable memTable;
    protected final List<PartitionedLevel> memoryLevels = new ArrayList<>();

    // protected final PriorityQueue<SSTable> memoryQueue =
    //new PriorityQueue<>((s1, s2) -> Long.compare(s1.minSeq, s2.minSeq));
    private int totalMemTableSize;
    protected int maxMemTableSize;

    protected final UnpartitionedLevel unpartitionedLevel;
    protected final List<PartitionedLevel> diskLevels = new ArrayList<>();

    protected final Config config;

    protected final KeyGenerator keyGen;
    protected final LinkedList<SSTable> memorySSTs = new LinkedList<>();
    protected final LinkedList<SSTable> diskSSTs = new LinkedList<>();

    protected long memoryMergeKeys = 0;
    protected long diskMergeKeys;

    public static int progress = 1000 * 1000; // 1 million
    protected int counter = 0;
    protected boolean loading = false;
    protected int loadKeys = 0;
    protected int writeKeys = 0;
    protected int numDiskFlushes = 0;

    protected int totalMemorySSTables = 0;
    protected int totalDiskSSTables = 0;

    protected long minSeq = 0;
    protected long nextSeq = 0;

    protected int totalDiskFlushes;
    protected int maxDiskFlushesPerLogTruncation;
    protected int totalLogTruncations;

    protected int totalUnpartitionedMerges;
    protected int totalUnpartitionedMergeGroups;

    protected List<Long> diskMergeKeysList = new ArrayList<>();

    protected PrintWriter printWriter;

    public LSMSimulator(KeyGenerator keyGen, Config config) {
        unpartitionedLevel = new UnpartitionedLevel(-1);
        this.keyGen = keyGen;
        this.config = config;
        this.memTableMap = new TreeMap<Long, Long>();
        this.memTable = new SSTable(config.memConfig.activeSize, true);
    }

    public void initializeMemoryLog(File file) throws IOException {
        printWriter = new PrintWriter(file);
    }

    public void resetStats() {
        maxMemTableSize = 0;
        memoryMergeKeys = 0;
        diskMergeKeys = 0;
        writeKeys = 0;
        numDiskFlushes = 0;
        diskMergeKeysList.clear();

        for (PartitionedLevel level : memoryLevels) {
            level.resetStats();
        }
        unpartitionedLevel.resetStats();

        totalDiskFlushes = 0;
        maxDiskFlushesPerLogTruncation = 0;
        totalLogTruncations = 0;

        totalUnpartitionedMerges = 0;
        totalUnpartitionedMergeGroups = 0;

        for (PartitionedLevel level : diskLevels) {
            level.resetStats();
        }
    }

    public void simulate(int totalKeys) {
        load();
        resetStats();
        for (writeKeys = 1; writeKeys <= totalKeys; writeKeys++) {
            int key = keyGen.nextKey();
            write(key);
            if (printWriter != null && writeKeys % MEMORY_INTERVAL == 0) {
                printWriter.println(writeKeys + "\t" + (nextSeq - minSeq) + "\t" + totalMemTableSize);
            }
        }
        if (printWriter != null) {
            printWriter.close();
        }
    }

    public void continueRun(int numKeys) {
        resetStats();
        for (writeKeys = 1; writeKeys <= numKeys; writeKeys++) {
            int key = keyGen.nextKey();
            write(key);
        }
    }

    protected void load() {
        loading = true;
        List<Long> list = new ArrayList<>(config.cardinality);
        for (long i = 0; i < config.cardinality; i++) {
            list.add(i);
        }
        Collections.shuffle(list, rand);
        for (loadKeys = 0; loadKeys < config.cardinality; loadKeys++) {
            write(list.get(loadKeys));
        }

        while (totalMemTableSize > 0 || !memTableMap.isEmpty()) {
            diskFlush(FlushReason.MEMORY);
        }

        loading = false;
    }

    protected void write(long key) {
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
            totalLogTruncations++;
            totalDiskFlushes += diskFlushed;
            maxDiskFlushesPerLogTruncation = Math.max(maxDiskFlushesPerLogTruncation, diskFlushed);

            if (VERBOSE && !loading && diskFlushed > 1) {
                System.out.println(totalLogTruncations + ": flushed " + diskFlushed + " sstables at once");
            }
        }

        numDiskFlushes += diskFlushed;

        if (memTableMap.size() >= config.memConfig.activeSize) {
            memoryFlush();
        }
        checkCounter();
    }

    protected void prepareMemoryFlush() {
        memTable.reset();
        for (Map.Entry<Long, Long> e : memTableMap.entrySet()) {
            memTable.add(e.getKey(), e.getValue());
        }
        memTableMap.clear();
        memTableMinSeq = Long.MAX_VALUE;

        increaseMemorySize(memTable.getSize());
    }

    protected void memoryFlush() {
        prepareMemoryFlush();

        if (memTable.getSize() >= config.memConfig.totalMemSize || !config.memConfig.enableMemoryMerge) {
            diskFlush(FlushReason.MEMORY);
        } else {
            boolean addLevel = memoryLevels.isEmpty()
                    || memoryLevels.get(0).getSize() > memTable.getSize() * config.memConfig.sizeRatio;
            Set<StorageUnit> overlappingSSTables = addLevel ? Collections.emptySet()
                    : Utils.findOverlappingSSTables(memTable, memoryLevels.get(0).sstables);

            MergeIterator iterator = new PartitionedIterator(memTable, overlappingSSTables);

            List<StorageUnit> newSSTables = buildSSTables(iterator, true);

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
        this.maxMemTableSize = Math.max(maxMemTableSize, totalMemTableSize);
    }

    protected void decreaseMemorySize(long size) {
        this.totalMemTableSize -= size;
    }

    protected abstract void diskFlush(FlushReason request);

    protected abstract GroupSelection selectGroupToMerge(UnpartitionedLevel unpartitionedLevel,
            List<PartitionedLevel> partitionedLevels);

    protected abstract void doUnpartitionedMerge(UnpartitionedLevel unpartitionedLevel, List<PartitionedLevel> levels,
            GroupSelection groupSelection);

    protected void updateMinSeq() {
        if (config.maxLogLength > 0) {
            minSeq = Math.min(nextSeq, memTableMinSeq);
            for (PartitionedLevel level : memoryLevels) {
                for (StorageUnit sstable : level.sstables) {
                    minSeq = Math.min(minSeq, ((SSTable) sstable).minSeq);
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
        Pair<StorageUnit, Set<StorageUnit>> pair = selector.selectMerge(this, current, next.sstables);

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
            List<StorageUnit> newSSTables = buildSSTables(iterator, isMemory);

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
                memoryMergeKeys += newKeys;
                int reduced = pair.getLeft().getSize() - increments;
                assert reduced >= 0;
                decreaseMemorySize(reduced);
                updateMinSeq();
            } else {
                diskMergeKeys += newKeys;
            }
        }
    }

    protected String printWriteAmplification(long mergedKeys) {
        return String.format("%.2f", (double) mergedKeys / (loading ? (loadKeys + 1) : (writeKeys + 1)));
    }

    public String printAverageFlushesPerLogTruncation() {
        return Utils.formatDivision(totalDiskFlushes, totalLogTruncations);
    }

    protected List<StorageUnit> buildSSTables(MergeIterator iterator, boolean isMemory) {
        KeyEntry entry = new KeyEntry();
        SSTable newSSTable = null;
        List<StorageUnit> newSSTables = new ArrayList<>();
        while (iterator.hasNext()) {
            iterator.getNext(entry);
            if (newSSTable == null) {
                newSSTable = getFreeSSTable(isMemory);
                newSSTables.add(newSSTable);
            }
            newSSTable.add(entry.key, entry.seq);
            if (newSSTable.isFull()) {
                newSSTable = null;
            }
        }
        return newSSTables;
    }

    protected SSTable getFreeSSTable(boolean isMemory) {
        LinkedList<SSTable> ssts = isMemory ? memorySSTs : diskSSTs;
        SSTable sstable = ssts.peekFirst();
        if (sstable == null) {
            sstable = new SSTable(isMemory ? config.memConfig.sstableSize : config.diskConfig.sstableSize, isMemory);
            if (isMemory) {
                totalMemorySSTables++;
            } else {
                totalDiskSSTables++;
            }
        } else {
            assert sstable.isFree;
            ssts.removeFirst();
        }
        sstable.reset();
        sstable.isFree = false;
        return sstable;
    }

    protected void freeSSTable(StorageUnit unit) {
        SSTable sstable = (SSTable) unit;
        LinkedList<SSTable> ssts = sstable.isMemory ? memorySSTs : diskSSTs;
        ssts.add(sstable);
        sstable.isFree = true;
    }

    protected void freeSSTables(Collection<? extends StorageUnit> sstables) {
        sstables.forEach(sst -> freeSSTable(sst));
    }

    protected void checkCounter() {
        counter++;
        if (counter == progress) {
            System.out.println("max memory table size " + maxMemTableSize);
            System.out.println("current memory table size " + totalMemTableSize);
            System.out.println("total memory sstables " + totalMemorySSTables);
            System.out.println("total disk sstables " + totalDiskSSTables);
            System.out.println(String.format("%s: completed %d keys. memory write amp %s, disk write amp %s",
                    loading ? "Load" : "Update", loading ? loadKeys : writeKeys,
                    printWriteAmplification(memoryMergeKeys), printWriteAmplification(diskMergeKeys)));
            System.out.println(printLevels());
            counter = 0;

            diskMergeKeysList.add(diskMergeKeys);
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

}
