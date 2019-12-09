package edu.uci.asterixdb.storage.sim;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Random;
import java.util.TreeSet;

import edu.uci.asterixdb.storage.sim.cache.Cache;
import edu.uci.asterixdb.storage.sim.cache.ICache;
import edu.uci.asterixdb.storage.sim.cache.OptimizedClockCache;
import edu.uci.asterixdb.storage.sim.cache.Page.PageState;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

class MemoryConfig {
    final int activeSize;
    final int sizeRatio;
    final boolean enableMemoryMerge;

    public MemoryConfig(int activeSize, int sizeRatio, boolean enableMemoryMerge) {
        super();
        this.activeSize = activeSize;
        this.sizeRatio = sizeRatio;
        this.enableMemoryMerge = enableMemoryMerge;
    }
}

class DiskConfig {
    final int sizeRatio;
    final int maxUnpartitionedSSTables;

    public DiskConfig(int sizeRatio, int maxUnpartitionedSSTables) {
        super();
        this.sizeRatio = sizeRatio;
        this.maxUnpartitionedSSTables = maxUnpartitionedSSTables;
    }
}

class LSMConfig {
    final MemoryConfig memConfig;
    final DiskConfig diskConfig;
    final int cardinality;

    public LSMConfig(MemoryConfig memConfig, DiskConfig diskConfig, int cardinality) {
        this.memConfig = memConfig;
        this.diskConfig = diskConfig;
        this.cardinality = cardinality;
    }
}

class TuningConfig {
    int writeMemSize;
    int cacheSize;
    final int simulateSize;
    final int pageSize;
    final double readWeight;
    final double writeWeight;
    final int tuningCycle;
    final int minMemorySize;
    final boolean enabled;

    public TuningConfig(int writeMemSize, int cacheSize, int simulateSize, int pageSize, double writeWeight,
            double readWeight, int tuningCycle, int minMemorySize, boolean enabled) {
        this.writeMemSize = writeMemSize;
        this.cacheSize = cacheSize;
        this.simulateSize = simulateSize;
        this.pageSize = pageSize;
        this.writeWeight = writeWeight;
        this.readWeight = readWeight;
        this.tuningCycle = tuningCycle;
        this.minMemorySize = minMemorySize;
        this.enabled = enabled;
    }
}

class Config {
    final LSMConfig[] lsmConfigs;
    final TuningConfig tuningConfig;
    final int memSSTableSize;
    final int diskSSTableSize;
    final long maxLogLength;

    public Config(LSMConfig[] lsmConfigs, TuningConfig tuningConfig, int memSSTableSize, int diskSSTableSize,
            long maxLogLength) {
        this.lsmConfigs = lsmConfigs;
        this.tuningConfig = tuningConfig;
        this.memSSTableSize = memSSTableSize;
        this.diskSSTableSize = diskSSTableSize;
        this.maxLogLength = maxLogLength;
    }
}

class LSMWorkload {
    final int writes;
    final int reads;
    final KeyGenerator writeGen;
    final KeyGenerator readGen;

    public LSMWorkload(int writes, int reads, KeyGenerator writeGen, KeyGenerator readGen) {
        this.writes = writes;
        this.reads = reads;
        this.writeGen = writeGen;
        this.readGen = readGen;
    }

    @Override
    public LSMWorkload clone() {
        return new LSMWorkload(writes, reads, writeGen.clone(), readGen.clone());
    }
}

class Workload {
    final int totalOps;
    final LSMWorkload[] workloads;

    public Workload(int totalOps, LSMWorkload[] workloads) {
        this.totalOps = totalOps;
        this.workloads = workloads;
    }

    public Workload(int totalOps, LSMWorkload workload) {
        this.totalOps = totalOps;
        this.workloads = new LSMWorkload[] { workload };
    }

    @Override
    public Workload clone() {
        LSMWorkload[] workloads = new LSMWorkload[this.workloads.length];
        for (int i = 0; i < workloads.length; i++) {
            workloads[i] = this.workloads[i].clone();
        }
        return new Workload(totalOps, workloads);
    }

}

class SimulateWorkload {
    final String name;
    final File file;
    final Workload[] workloads;

    public SimulateWorkload(String name, File file, Workload[] workloads) {
        this.name = name;
        this.file = file;
        this.workloads = workloads;
    }

    public SimulateWorkload(String name, File file, Workload workload) {
        this(name, file, new Workload[] { workload });
    }

    @Override
    public SimulateWorkload clone() {

        Workload[] workloads = new Workload[this.workloads.length];
        for (int i = 0; i < workloads.length; i++) {
            workloads[i] = this.workloads[i].clone();
        }

        return new SimulateWorkload(name, file, workloads);
    }

    public void initCardinality(LSMConfig[] lsmConfigs) {
        for (Workload workload : workloads) {
            for (int i = 0; i < workload.workloads.length; i++) {
                LSMWorkload lsmWorkload = workload.workloads[i];
                lsmWorkload.writeGen.initCard(lsmConfigs[i].cardinality);
                lsmWorkload.readGen.initCard(lsmConfigs[i].cardinality);
            }
        }
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
    int numMemoryFlushes;
    int numLogFlushes;
    long memoryMergeKeys = 0;
    long diskMergeKeys = 0;
    int totalMemorySSTables = 0;
    int totalDiskSSTables = 0;
    int maxDiskFlushesPerLogTruncation = 0;
    int totalLogTruncations = 0;
    int totalUnpartitionedMerges = 0;
    int totalUnpartitionedMergeGroups = 0;
    int maxMemTableSize = 0;

    long totalFlushedSize;

    public void updateMaxWriteMem(int writeMem) {
        this.maxMemTableSize = Math.max(maxMemTableSize, writeMem);
    }
}

public class Simulator {
    public enum FlushReason {
        MEMORY,
        LOG
    }

    public static boolean ROUND_ROBIN = false;

    public static boolean VERBOSE = false;

    public static int MEMORY_INTERVAL = 1000;

    protected static final TreeSet<SSTable> Empty_TreeSet = new TreeSet<>();

    protected final Random rand = new Random(17);

    protected final ArrayDeque<MemorySSTable> memorySSTs = new ArrayDeque<>();
    protected final ArrayDeque<DiskSSTable> diskSSTs = new ArrayDeque<>();

    public static int progress = 1000 * 1000; // 1 million
    protected boolean loading = false;

    protected int usedWriteMem;
    // logging
    protected int minSeq = 0;
    protected int nextSeq = 0;

    protected PrintWriter tuningPrintWriter;
    protected SimulationStats stats = new SimulationStats();

    protected final Cache cache;
    protected long reads;
    protected long writes;
    protected final MemoryTuner tuner;
    protected final Config config;
    protected final SimulatedLSM[] lsmTrees;

    public Simulator(Config config) {
        this.config = config;
        lsmTrees = new SimulatedLSM[config.lsmConfigs.length];
        for (int i = 0; i < lsmTrees.length; i++) {
            lsmTrees[i] = new SimulatedLSM(this, config.lsmConfigs[i]);
        }

        ICache cache =
                new OptimizedClockCache(config.tuningConfig.cacheSize / config.tuningConfig.pageSize, PageState.CACHED);
        ICache simulateCache = new OptimizedClockCache(config.tuningConfig.simulateSize / config.tuningConfig.pageSize,
                PageState.CACHED_SIMULATE);
        this.cache = new Cache(cache, simulateCache);

        this.tuner = new MemoryTuner(this);
    }

    public void initializeTuningLog(File file) throws IOException {
        tuningPrintWriter = new PrintWriter(file);
    }

    public void resetStats() {
        for (int i = 0; i < lsmTrees.length; i++) {
            lsmTrees[i].resetStats();
        }
        stats = new SimulationStats();
        cache.resetStats();
    }

    public void load(File file) throws IOException {
        loading = true;
        reads = 0;
        writes = 0;

        for (int i = 0; i < lsmTrees.length; i++) {
            LSMConfig lsmConfig = config.lsmConfigs[i];
            IntList list = new IntArrayList(lsmConfig.cardinality);
            for (int k = 0; k < lsmConfig.cardinality; k++) {
                list.add(k);
            }
            IntLists.shuffle(list, rand);
            for (int k = 0; k < lsmConfig.cardinality; k++, writes++) {
                write(lsmTrees[i], list.getInt(k));
            }
        }

        while (usedWriteMem > 0) {
            diskFlush(FlushReason.MEMORY);
        }

        loading = false;
        serialize(file);
    }

    public void simulate(SimulateWorkload simulateWorkload) throws IOException {
        resetStats();
        deserialize(simulateWorkload.file);
        simulateWorkload.initCardinality(config.lsmConfigs);
        reads = 0;
        writes = 0;
        long lastWrites = 0;
        long lastMinSeq = 0;

        long totalOps = 0;
        for (Workload workload : simulateWorkload.workloads) {
            totalOps += workload.totalOps;
            while (writes < totalOps) {
                for (int i = 0; i < workload.workloads.length; i++) {
                    LSMWorkload lsmWorkload = workload.workloads[i];
                    for (int w = 0; w < lsmWorkload.writes; w++, writes++) {
                        write(lsmTrees[i], lsmWorkload.writeGen.nextKey());
                    }
                    for (int r = 0; r < lsmWorkload.reads; r++, reads++) {
                        read(lsmTrees[i], lsmWorkload.readGen.nextKey());
                    }
                }

                // check tuning
                if (minSeq > lastMinSeq + config.tuningConfig.tuningCycle
                        && writes > lastWrites + config.tuningConfig.tuningCycle) {
                    if (config.tuningConfig.enabled) {
                        tuner.tune();
                    }

                    lastMinSeq = minSeq;
                    lastWrites = writes;
                }
            }

        }

        if (tuningPrintWriter != null) {
            tuningPrintWriter.close();
        }
    }

    protected void deserialize(File file) throws IOException {
        DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        int numTrees = input.readInt();
        if (numTrees != config.lsmConfigs.length) {
            input.close();
            throw new IllegalStateException(
                    "Mismatched #LSM trees. Expected " + config.lsmConfigs.length + " actual " + numTrees);
        }

        for (int i = 0; i < numTrees; i++) {
            lsmTrees[i].deserialize(input);
        }
        input.close();
    }

    protected void serialize(File file) throws IOException {
        // persist
        DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
        // for verification
        output.writeInt(lsmTrees.length);
        for (int i = 0; i < lsmTrees.length; i++) {
            lsmTrees[i].serialize(output);
        }
        output.close();
    }

    protected void write(SimulatedLSM lsm, int key) {
        checkCounter();

        lsm.write(key, nextSeq++);
        int diskFlushed = 0;
        // do log flush
        if (config.maxLogLength > 0 && minSeq + config.maxLogLength < nextSeq) {
            while (nextSeq > minSeq + config.maxLogLength) {
                diskFlush(FlushReason.LOG);
                diskFlushed++;
            }
            stats.totalLogTruncations++;
            stats.maxDiskFlushesPerLogTruncation = Math.max(stats.maxDiskFlushesPerLogTruncation, diskFlushed);

            if (VERBOSE && !loading && diskFlushed > 1) {
                System.out.println(stats.totalLogTruncations + ": flushed " + diskFlushed + " sstables at once");
            }
        }
        // do memory flush
        while (usedWriteMem >= config.tuningConfig.writeMemSize) {
            diskFlush(FlushReason.MEMORY);
        }
    }

    protected void read(SimulatedLSM lsm, int key) {
        checkCounter();
        lsm.read(key);
    }

    protected void diskFlush(FlushReason reason) {
        SimulatedLSM lsm = getMinSeqLSM();
        lsm.diskFlush(reason);
    }

    protected void recomputeMinSeq() {
        SimulatedLSM lsm = getMinSeqLSM();
        minSeq = lsm.minSeq;
    }

    private SimulatedLSM getMinSeqLSM() {
        SimulatedLSM minLSM = lsmTrees[0];
        for (int i = 1; i < lsmTrees.length; i++) {
            if (lsmTrees[i].minSeq < minLSM.minSeq) {
                minLSM = lsmTrees[i];
            }
        }
        return minLSM;
    }

    protected String printWriteAmplification(long mergedKeys) {
        return String.format("%.2f", (double) mergedKeys / writes);
    }

    public String printAverageFlushesPerLogTruncation() {
        return Utils.formatDivision(stats.numLogFlushes, stats.totalLogTruncations);
    }

    protected SSTable getFreeSSTable(SimulatedLSM lsm, boolean isMemory, int level) {
        ArrayDeque<? extends SSTable> ssts = isMemory ? memorySSTs : diskSSTs;
        SSTable sstable = ssts.peekFirst();
        if (sstable == null) {
            if (isMemory) {
                sstable = new MemorySSTable(config.memSSTableSize);
                stats.totalMemorySSTables++;
            } else {
                sstable = new DiskSSTable(config.diskSSTableSize, this);
                stats.totalDiskSSTables++;
            }
        } else {
            assert sstable.isFree;
            ssts.removeFirst();
        }
        sstable.reset(level);
        sstable.isFree = false;
        sstable.lsm = lsm;
        return sstable;
    }

    protected void freeSSTable(SSTable sstable) {
        if (sstable instanceof MemorySSTable) {
            memorySSTs.add((MemorySSTable) sstable);
        } else {
            diskSSTs.add((DiskSSTable) sstable);
        }
        sstable.deletePages();
        sstable.isFree = true;
    }

    protected void freeSSTables(Collection<SSTable> sstables) {
        sstables.forEach(sst -> freeSSTable(sst));
    }

    protected void checkCounter() {
        if (writes + reads > 0 && (writes + reads) % progress == 0) {
            synchronized (Simulator.class) {
                System.out.println("max memory table size " + stats.maxMemTableSize);
                System.out.println("current memory table size " + usedWriteMem);
                System.out.println("total memory sstables " + stats.totalMemorySSTables);
                System.out.println("total disk sstables " + stats.totalDiskSSTables);
                System.out.println(String.format("%s: completed %d keys. memory write amp %s, disk write amp %s",
                        loading ? "Load" : "Update", (writes + reads), printWriteAmplification(stats.memoryMergeKeys),
                        printWriteAmplification(stats.diskMergeKeys)));
                for (SimulatedLSM lsm : lsmTrees) {
                    System.out.println(lsm.printLevels());
                }
            }
        }
    }

    public void updateMemoryComponentSize(int newSize) {
        config.tuningConfig.writeMemSize = newSize;
        while (usedWriteMem > config.tuningConfig.writeMemSize) {
            diskFlush(FlushReason.MEMORY);
        }
    }

    public void updateBufferCacheSize(int newSize) {
        config.tuningConfig.cacheSize = newSize;
        cache.resize(newSize / config.tuningConfig.pageSize, p -> {
        });
    }

}
