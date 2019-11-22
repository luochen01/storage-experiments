package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import edu.uci.asterixdb.storage.sim.LSMSimulator.CachePolicy;

class TuningExperiment extends Experiment {
    public static final int PAGE_SIZE = 4;
    public static final int SIMULATE_CACHE_SIZE = 65536;
    public static final int MIN_MEMORY_SIZE = 32 * 1024;

    public static void main(String[] args) throws IOException {
        //runPaper();
        //runCachePolicy();
        runTuner();
    }

    public static void runTuner() throws IOException {
        LSMSimulator.progress = Integer.MAX_VALUE;
        SIM_FACTOR = 10;

        executor.setCorePoolSize(1);

        List<Config> adaptiveConfigs = new ArrayList<>();

        DiskConfig disk = new DiskConfig(8 * 1024, 10, 0, false);
        // 10M keys
        long log = 1 * 1024 * 1024l;
        //long log = Integer.MAX_VALUE;
        int memory = 4 * 1024 * 1024; // 4G

        Integer[] writeMems = new Integer[] { 4000 * 1024 };
        //Integer[] writeMems = new Integer[] { 2 * 1024 * 1024 };

        int reads = 1;
        int writes = 1;

        double writeWeight = 1;
        double readWeight = 1;

        int tuningCycle = (int) log;
        long excludedCycles = 0;
        for (int mem : writeMems) {
            adaptiveConfigs.add(new Config(new MemoryConfig(8192, mem, 8192, 10, true), disk,
                    new TuningConfig(memory - mem, SIMULATE_CACHE_SIZE, PAGE_SIZE, writes, reads, writeWeight,
                            readWeight, CachePolicy.ADAPTIVE, tuningCycle, MIN_MEMORY_SIZE, true, excludedCycles),
                    CARD, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] writeGens = new KeyGenerator[] { new UniformGenerator() };
        KeyGenerator[] readGens = new KeyGenerator[] { new ScrambleZipfGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        File file = new File("tuning_db");

        for (int i = 0; i < writeGens.length; i++) {
            parallelSimulations(adaptiveConfigs, writeGens[i], readGens[i], "adaptive", writeMems, maps, futures, false,
                    file);
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

    private static void runCachePolicy() throws IOException {
        executor.setCorePoolSize(4);

        List<Config> bypassConfigs = new ArrayList<>();
        List<Config> adaptiveConfigs = new ArrayList<>();
        List<Config> writebackConfigs = new ArrayList<>();

        DiskConfig disk = new DiskConfig(8 * 1024, 10, 0, false);
        // 10M keys
        long log = 2 * 1024 * 1024l;
        int memory = 4 * 1024 * 1024; // 4G

        Integer[] writeMems = new Integer[] { 128 * 1024, 256 * 1024, 512 * 1024, 1 * 1024 * 1024, 2 * 1024 * 1024,
                3 * 1024 * 1024, 3584 * 1024 };
        //Integer[] writeMems = new Integer[] { 3 * 1024 * 1024 };

        int reads = 1;
        int writes = 1;

        double weight = 1;

        for (int mem : writeMems) {
            bypassConfigs.add(new Config(new MemoryConfig(8192, mem, 8192, 10, true), disk,
                    new TuningConfig(memory - mem, SIMULATE_CACHE_SIZE, PAGE_SIZE, writes, reads, weight, weight,
                            CachePolicy.BYPASS, Integer.MAX_VALUE, MIN_MEMORY_SIZE, false, 0),
                    CARD, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
            adaptiveConfigs.add(new Config(new MemoryConfig(8192, mem, 8192, 10, true), disk,
                    new TuningConfig(memory - mem, SIMULATE_CACHE_SIZE, PAGE_SIZE, writes, reads, weight, weight,
                            CachePolicy.ADAPTIVE, Integer.MAX_VALUE, MIN_MEMORY_SIZE, false, 0),
                    CARD, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
            writebackConfigs.add(new Config(new MemoryConfig(8192, mem, 8192, 10, true), disk,
                    new TuningConfig(memory - mem, SIMULATE_CACHE_SIZE, PAGE_SIZE, writes, reads, weight, weight,
                            CachePolicy.WRITE_BACK, Integer.MAX_VALUE, MIN_MEMORY_SIZE, false, 0),
                    CARD, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] writeGens = new KeyGenerator[] { new UniformGenerator() };
        KeyGenerator[] readGens = new KeyGenerator[] { new ZipfGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        File file = new File("tuning_db");
        //        loadSimulation(partitionedConfigs.get(0), file, keyGens[0]);

        for (int i = 0; i < writeGens.length; i++) {
            //            parallelSimulations(bypassConfigs, writeGens[i], readGens[i], "bypass", writeMems, maps, futures, false,
            //                    file);
            parallelSimulations(writebackConfigs, writeGens[i], readGens[i], "writeback", writeMems, maps, futures,
                    false, file);
            parallelSimulations(adaptiveConfigs, writeGens[i], readGens[i], "adaptive", writeMems, maps, futures, false,
                    file);
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

    private static void runPaper() throws IOException {
        List<Config> partitionedConfigs = new ArrayList<>();

        DiskConfig disk = new DiskConfig(8 * 1024, 10, 0, false);
        // 10M keys
        long log = 2 * 1024 * 1024l;
        int memory = 4 * 1024 * 1024; // 4G

        Integer[] writeMems = new Integer[9];
        for (int i = 0; i < writeMems.length; i++) {
            writeMems[i] = memory / 10 * (i + 1);
        }

        int reads = 4;
        int writes = 1;
        double weight = 1;

        for (int mem : writeMems) {
            partitionedConfigs.add(new Config(new MemoryConfig(8192, mem, 8192, 10, true), disk,
                    new TuningConfig(memory - mem, SIMULATE_CACHE_SIZE, PAGE_SIZE, writes, reads, weight, weight,
                            CachePolicy.ADAPTIVE, Integer.MAX_VALUE, MIN_MEMORY_SIZE, false, 0),
                    CARD, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ScrambleZipfGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        File file = new File("tuning_db");
        //        loadSimulation(partitionedConfigs.get(0), file, keyGens[0]);

        for (KeyGenerator keyGen : keyGens) {
            parallelSimulations(partitionedConfigs, keyGen, keyGen, "allocation", writeMems, maps, futures, false,
                    file);
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
}