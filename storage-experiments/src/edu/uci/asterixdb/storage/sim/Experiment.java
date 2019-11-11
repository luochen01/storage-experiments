package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class Experiment {
    private static final MemoryConfig SMALL_MEMORY = new MemoryConfig(64, 1 * 1024, 64, 10, true);
    private static final DiskConfig SMALL_DISK = new DiskConfig(64, 10, 2, false);
    private static final int SMALL_CARD = 1 * 1000 * 1000;
    private static final Config SMALL_CONF = new Config(SMALL_MEMORY, SMALL_DISK, SMALL_CARD,
            RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0);

    private static final MemoryConfig BTREE_MEMORY = new MemoryConfig(100 * 1024 / 3 * 2, 100 * 1024, 4096, 10, false);
    private static final MemoryConfig MERGE_MEMORY = new MemoryConfig(4 * 1024, 100 * 1024, 4096, 10, true);
    private static final DiskConfig DISK = new DiskConfig(4096, 10, 0, false);
    private static final int CARD = 10 * 1000 * 1000;
    private static final Config CONF =
            new Config(MERGE_MEMORY, DISK, CARD, RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0);

    private static final int NUM_THREADS = 2;
    private static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    public static void main(String[] args) {
        runPaper();
        // runLogMemory();
        //compareLogStrategy();
        // compareLogStrategy();
        // runMemorySizeForAddLevel();
        //        runMultipleUnpartitioned();
        // compareLog();
        // debugStrategy();
        //  compareLogStrategy();

        // compareMemory();
    }

    private static void runPaper() {
        List<Config> partitionedConfigs = new ArrayList<>();
        List<Config> btreeConfigs = new ArrayList<>();

        DiskConfig disk = new DiskConfig(64 * 1024, 10, 0, false);
        // 100M keys
        int card = 100 * 1024 * 1024;
        long log = 512 * 1024l;
        Integer[] memories = new Integer[] { 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024, 4096 * 1024,
                8192 * 1024 };
        //Integer[] memories = new Integer[] { 512 * 1024 };
        for (Integer mem : memories) {
            partitionedConfigs.add(new Config(new MemoryConfig(8192, mem, 8192, 10, true), disk, card,
                    HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
            btreeConfigs.add(new Config(new MemoryConfig((int) (mem * 0.69), (int) (mem * 0.69), 8192, 10, false), disk,
                    card, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ZipfGenerator() };

        //KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (KeyGenerator keyGen : keyGens) {
            parallelSimulations(partitionedConfigs, keyGen, "partitioned", memories, maps, futures, false);
            parallelSimulations(btreeConfigs, keyGen, "full", memories, maps, futures, false);
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        maps.forEach(m -> printMap(m));

        executor.shutdown();
    }

    private static void runLogMemory() {
        List<Config> configs = new ArrayList<>();

        long log = 512 * 1024l;
        Integer[] memories = new Integer[] { 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024 };
        //Integer[] memories = new Integer[] { 512 * 1024 };
        for (Integer mem : memories) {
            configs.add(new Config(new MemoryConfig(4096, mem, 4096 * 2, 10, true), DISK, CARD, HybridSelector.INSTANCE,
                    GreedySelector.INSTANCE, log, log));
        }

        //KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ZipfGenerator() };

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (KeyGenerator keyGen : keyGens) {
            parallelSimulations(configs, keyGen, "memory", memories, maps, futures, true);
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        maps.forEach(m -> printMap(m));

        executor.shutdown();
    }

    private static void compareLogStrategy() {
        List<Config> roundRobinConfigs = new ArrayList<>();
        List<Config> oldestSeqConfigs = new ArrayList<>();
        List<Config> greedyConfigs = new ArrayList<>();

        Long[] logs = new Long[] { 64 * 1024l, 128 * 1024l, 256 * 1024l, 512 * 1024l, 1024 * 1024l, 2048 * 1024l };
        //Long[] logs = new Long[] { 256 * 1024l };
        for (Long log : logs) {
            roundRobinConfigs.add(new Config(new MemoryConfig(4096, Integer.MAX_VALUE, 4096, 10, true), DISK, CARD,
                    RoundRobinSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
            oldestSeqConfigs.add(new Config(new MemoryConfig(4096, Integer.MAX_VALUE, 4096, 10, true), DISK, CARD,
                    OldestMinLSNSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
            greedyConfigs.add(new Config(new MemoryConfig(4096, Integer.MAX_VALUE, 4096, 10, true), DISK, CARD,
                    GreedySelector.INSTANCE, GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ZipfGenerator() };
        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (KeyGenerator keyGen : keyGens) {
            parallelSimulations(roundRobinConfigs, keyGen, "round-robin", logs, maps, futures, false);
            parallelSimulations(oldestSeqConfigs, keyGen, "oldest-seq", logs, maps, futures, false);
            parallelSimulations(greedyConfigs, keyGen, "greedy", logs, maps, futures, false);
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        maps.forEach(m -> printMap(m));

        executor.shutdown();
    }

    private static void runMultipleUnpartitioned() {
        List<Config> configs64 = new ArrayList<>();
        List<Config> configs64RoundRobin = new ArrayList<>();

        List<Config> configs256 = new ArrayList<>();
        List<Config> configs1024 = new ArrayList<>();

        Integer[] params = new Integer[] { 1, 2, 3, 4, 5, 10, 20 };

        // Integer[] params = new Integer[] { 10 };
        for (Integer param : params) {
            DiskConfig disk = new DiskConfig(4096, 10, param, false);

            int logLength = 0;
            configs64.add(new Config(new MemoryConfig(4096, 64 * 1024, 4096, 10, true), disk, CARD,
                    HybridSelector.INSTANCE, RoundRobinSelector.INSTANCE, logLength, logLength));

            configs64RoundRobin.add(new Config(new MemoryConfig(4096, 64 * 1024, 4096, 10, true), disk, CARD,
                    RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, logLength, logLength));

            configs256.add(new Config(new MemoryConfig(4096, 256 * 1024, 4096, 10, true), disk, CARD,
                    HybridSelector.INSTANCE, RoundRobinSelector.INSTANCE, logLength, logLength));

            configs1024.add(new Config(new MemoryConfig(4096, 1024 * 1024, 4096, 10, true), disk, CARD,
                    HybridSelector.INSTANCE, RoundRobinSelector.INSTANCE, logLength, logLength));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator() };
        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (KeyGenerator keyGen : keyGens) {
            parallelSimulations(configs64, keyGen, "64K", params, maps, futures, false);
            parallelSimulations(configs64RoundRobin, keyGen, "64K-round-robin", params, maps, futures, false);

            //parallelSimulations(configs256, keyGen, "256K", params, maps, futures);
            //parallelSimulations(configs1024, keyGen, "1024K", params, maps, futures);
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        maps.forEach(m -> printMap(m));

        executor.shutdown();
    }

    private static void compareStrategy() {
        List<Config> roundRobinConfigs = new ArrayList<>();
        List<Config> minSeqConfigs = new ArrayList<>();
        List<Config> minOverlapConfigs = new ArrayList<>();

        int memory = 4 * 1024;
        Integer[] cards = new Integer[] { 10 * 1000 * 1000, 20 * 1000 * 1000, 30 * 1000 * 1000, 40 * 1000 * 1000,
                50 * 1000 * 1000 };
        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ScrambleZipfGenerator() };

        for (int card : cards) {
            roundRobinConfigs.add(new Config(new MemoryConfig(memory, memory, 4096, 10, false), DISK, card,
                    RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
            //minSeqConfigs.add(new Config(new MemoryConfig(memory, memory, 4096, 10, false), DISK, card,
            //        OldestMinSeqSelector.INSTANCE, 0));
            minOverlapConfigs.add(new Config(new MemoryConfig(memory, memory, 4096, 10, false), DISK, card,
                    GreedySelector.INSTANCE, GreedySelector.INSTANCE, 0, 0));
        }

        List<String> results = new ArrayList<>();
        for (KeyGenerator keyGen : keyGens) {
            runSimulations(roundRobinConfigs, keyGen, results, "round-robin", cards);
            runSimulations(minSeqConfigs, keyGen, results, "min-seq", cards);
            runSimulations(minOverlapConfigs, keyGen, results, "min-overlap", cards);
        }

        results.forEach(l -> System.out.println(l));
    }

    private static void runChangingMemory() {
        MemoryConfig memory = new MemoryConfig(4096, 1024 * 1024, 4096, 10, true);
        Config config = new Config(memory, DISK, CARD, RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0);

        LSMSimulator.VERBOSE = false;
        KeyGenerator gen = new UniformGenerator();
        LSMSimulator sim = new LSMSimulatorHorizontal(gen, config);

        sim.simulate(CARD);

        LSMSimulator.VERBOSE = true;
        LSMSimulator.progress = 64 * 1024;
        memory.totalMemSize = 64 * 1024;
        sim.continueRun(CARD);

        for (int i = 0; i < sim.diskMergeKeysList.size(); i++) {
            long prev = i == 0 ? 0 : sim.diskMergeKeysList.get(i - 1);
            long mergedKeys = sim.diskMergeKeysList.get(i) - prev;

            System.out.println(i * LSMSimulator.progress + "\t" + mergedKeys + "\t"
                    + Utils.formatDivision(mergedKeys, LSMSimulator.progress));
        }

    }

    private static void runMemorySizeForAddLevel() {
        List<Config> staticHybridConfigs = new ArrayList<>();
        List<Config> staticRoundRobinConfigs = new ArrayList<>();
        List<Config> dynamicConfigs = new ArrayList<>();
        Integer[] memories = new Integer[] { 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024 };
        for (int memory : memories) {
            staticHybridConfigs
                    .add(new Config(new MemoryConfig(4096, memory, 4096, 10, true), new DiskConfig(4096, 10, 0, false),
                            CARD, HybridSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
            staticRoundRobinConfigs
                    .add(new Config(new MemoryConfig(4096, memory, 4096, 10, true), new DiskConfig(4096, 10, 0, false),
                            CARD, RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
            dynamicConfigs
                    .add(new Config(new MemoryConfig(4096, memory, 4096, 10, true), new DiskConfig(4096, 10, 0, true),
                            CARD, RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
        }

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ScrambleZipfGenerator() };
        for (KeyGenerator keyGen : keyGens) {
            // parallelSimulations(staticHybridConfigs, keyGen, "static", memories, maps, futures);
            //parallelSimulations(dynamicConfigs, keyGen, "dynamic", memories, maps, futures);
            parallelSimulations(staticRoundRobinConfigs, keyGen, "static", memories, maps, futures, false);
        }
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        maps.forEach(m -> printMap(m));

        executor.shutdown();
    }

    private static void compareMemory() {
        List<Config> btreeConfigs = new ArrayList<>();
        List<Config> btreeFullConfigs = new ArrayList<>();
        List<Config> partitionConfigs = new ArrayList<>();

        Integer[] memories = new Integer[] { 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024 };

        for (int memory : memories) {
            btreeConfigs.add(new Config(new MemoryConfig((int) (memory * 0.7), (int) (memory * 0.7), 4096, 10, false),
                    DISK, CARD, RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
            btreeFullConfigs.add(new Config(new MemoryConfig(memory, memory, 4096, 10, false), DISK, CARD,
                    RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
            partitionConfigs.add(new Config(new MemoryConfig(4 * 1024, memory, 4096, 10, true), DISK, CARD,
                    RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ZipfGenerator() };
        List<String> results = new ArrayList<>();

        for (KeyGenerator keyGen : keyGens) {
            results.add(keyGen.toString());
            //runSimulations(btreeConfigs, keyGen, results, "btree-fragmented", memories);
            //runSimulations(btreeFullConfigs, keyGen, results, "btree-full", memories);
            runSimulations(partitionConfigs, keyGen, results, "partitioned", memories);
        }

        results.forEach(l -> System.out.println(l));
    }

    private static void compareLog() {
        List<Config> btreeConfigs = new ArrayList<>();
        List<Config> partitionConfigs = new ArrayList<>();

        Long[] logs = new Long[] { 64 * 1024l, 128 * 1024l, 256 * 1024l, 512 * 1024l, 1024 * 1024l, 2048 * 1024l,
                4096 * 1024l };

        for (long log : logs) {
            btreeConfigs.add(new Config(new MemoryConfig((int) log, Integer.MAX_VALUE, 4096, 10, false), DISK, CARD,
                    RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, log, log));
            partitionConfigs.add(new Config(new MemoryConfig(4096, Integer.MAX_VALUE, 4096, 10, true), DISK, CARD,
                    RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, log, log));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ZipfGenerator() };
        List<String> results = new ArrayList<>();

        for (KeyGenerator keyGen : keyGens) {
            runSimulations(btreeConfigs, keyGen, results, "btree", logs);
            runSimulations(partitionConfigs, keyGen, results, "partition", logs);
        }

        results.forEach(l -> System.out.println(l));
    }

    private static void runSimulations(List<Config> configs, KeyGenerator gen, List<String> results, String name,
            Object[] params) {
        results.add(name);
        results.add("param\tmemory amp\tdisk ampt\tmemory size");
        for (int i = 0; i < configs.size(); i++) {
            Config config = configs.get(i);
            gen.initCard(config.cardinality);
            LSMSimulator sim = new LSMSimulatorHorizontal(gen, config);
            sim.simulate(config.cardinality);
            results.add(params[i] + "\t" + sim.printWriteAmplification(sim.memoryMergeKeys) + "\t"
                    + sim.printWriteAmplification(sim.diskMergeKeys) + "\t" + sim.maxMemTableSize);
        }
    }

    private static void parallelSimulations(List<Config> configs, KeyGenerator gen, String name, Object[] params,
            List<Map<Integer, String>> resultMaps, List<Future<?>> futures, boolean collectMemoryLog) {
        Map<Integer, String> results = Collections.synchronizedMap(new HashMap<>());
        resultMaps.add(results);
        results.put(0, gen + "\t" + name);
        results.put(1, "param\tmemory amp\tdisk ampt\tmemory size\taverage disk flushes\tmax disk flushes");
        for (int i = 0; i < configs.size(); i++) {
            Config config = configs.get(i);
            final KeyGenerator newGen = gen.clone();
            final int index = i + 2;
            final Object param = params[i];
            Future<?> future = executor.submit(() -> {
                System.out.println(String.format("starting %s + %s + %s", newGen.toString(), name, param.toString()));
                newGen.initCard(config.cardinality);
                LSMSimulator sim = new LSMSimulatorHorizontal(newGen, config);
                if (collectMemoryLog) {
                    try {
                        sim.initializeMemoryLog(
                                new File(newGen.toString() + "-" + name + "-" + param.toString() + ".log"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                sim.simulate(config.cardinality);
                results.put(index, param + "\t" + sim.printWriteAmplification(sim.memoryMergeKeys) + "\t"
                        + sim.printWriteAmplification(sim.diskMergeKeys) + "\t" + sim.maxMemTableSize + "\t"
                        + sim.printAverageFlushesPerLogTruncation() + "\t" + sim.maxDiskFlushesPerLogTruncation + "\t"
                        + Utils.formatDivision(sim.totalUnpartitionedMergeGroups, sim.totalUnpartitionedMerges));
            });
            futures.add(future);
        }
    }

    private static void printMap(Map<Integer, String> map) {
        for (int i = 0; i < map.size(); i++) {
            System.out.println(map.get(i));
        }
    }

}