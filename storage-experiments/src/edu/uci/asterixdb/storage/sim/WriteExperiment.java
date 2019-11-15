package edu.uci.asterixdb.storage.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

class WriteExperiment extends Experiment {

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
        long log = 10 * 1024 * 1024l;
        Integer[] memories = new Integer[] { 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024,
                4096 * 1024, 8192 * 1024 };
        for (Integer mem : memories) {
            partitionedConfigs.add(new Config(new MemoryConfig(8192 * 4, mem, 8192, 10, true), disk, card,
                    HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
            btreeConfigs.add(new Config(new MemoryConfig((int) (mem * 0.69), (int) (mem * 0.69), 8192, 10, false), disk,
                    card, HybridSelector.INSTANCE, GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator(), new ZipfGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        //LSMSimulatorHorizontal.ROUND_ROBIN = true;
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

}