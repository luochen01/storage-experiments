package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class Experiment {
    public static final MemoryConfig SMALL_MEMORY = new MemoryConfig(64, 1 * 1024, 64, 10, true);
    public static final DiskConfig SMALL_DISK = new DiskConfig(64, 10, 2, false);
    public static final int SMALL_CARD = 1 * 1000 * 1000;
    public static final Config SMALL_CONF = new Config(SMALL_MEMORY, SMALL_DISK, SMALL_CARD,
            RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0);

    public static final MemoryConfig BTREE_MEMORY = new MemoryConfig(100 * 1024 / 3 * 2, 100 * 1024, 4096, 10, false);
    public static final MemoryConfig MERGE_MEMORY = new MemoryConfig(4 * 1024, 100 * 1024, 4096, 10, true);
    public static final DiskConfig DISK = new DiskConfig(4096, 10, 0, false);
    public static final int CARD = 10 * 1000 * 1000;
    public static final Config CONF =
            new Config(MERGE_MEMORY, DISK, CARD, RoundRobinSelector.INSTANCE, RoundRobinSelector.INSTANCE, 0, 0);

    public static final int NUM_THREADS = 2;
    public static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    public static void parallelSimulations(List<Config> configs, KeyGenerator gen, String name, Object[] params,
            List<Map<Integer, String>> resultMaps, List<Future<?>> futures, boolean collectMemoryLog) {
        Map<Integer, String> results = Collections.synchronizedMap(new HashMap<>());
        resultMaps.add(results);
        results.put(0, gen + "\t" + name);
        results.put(1,
                "param\tmemory amp\tdisk ampt\tmemory size\taverage disk flushes\tmax disk flushes\tdisk reads\tdisk writes");
        for (int i = 0; i < configs.size(); i++) {
            Config config = configs.get(i);
            final KeyGenerator newGen = gen.clone();
            final int index = i + 2;
            final Object param = params[i];
            Future<?> future = executor.submit(() -> {
                System.out.println(String.format("starting %s + %s + %s", newGen.toString(), name, param.toString()));
                newGen.initCard(config.cardinality);
                LSMSimulator sim = new LSMSimulator(newGen, config);
                if (collectMemoryLog) {
                    try {
                        sim.initializeMemoryLog(
                                new File(newGen.toString() + "-" + name + "-" + param.toString() + ".log"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                sim.simulate(config.cardinality);
                SimulationStats stats = sim.stats;
                results.put(index, param + "\t" + sim.printWriteAmplification(stats.memoryMergeKeys) + "\t"
                        + sim.printWriteAmplification(stats.diskMergeKeys) + "\t" + stats.maxMemTableSize + "\t"
                        + sim.printAverageFlushesPerLogTruncation() + "\t" + stats.maxDiskFlushesPerLogTruncation + "\t"
                        + Utils.formatDivision(stats.totalUnpartitionedMergeGroups, stats.totalUnpartitionedMerges)
                        + "\t" + sim.cache.getDiskReads() + "\t" + sim.cache.getDiskWrites());
            });
            futures.add(future);
        }
    }

    public static void printMap(Map<Integer, String> map) {
        for (int i = 0; i < map.size(); i++) {
            System.out.println(map.get(i));
        }
    }

}