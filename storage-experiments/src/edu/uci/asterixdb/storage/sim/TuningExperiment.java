package edu.uci.asterixdb.storage.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

class TuningExperiment extends Experiment {
    public static final int PAGE_SIZE = 4;

    public static final int MAX_PAGES = 1 * 128 * 1024; // 4GB

    public static void main(String[] args) {
        runPaper();
    }

    private static void runPaper() {
        List<Config> partitionedConfigs = new ArrayList<>();

        DiskConfig disk = new DiskConfig(64 * 1024, 10, 0, false);
        // 10M keys
        long log = 10 * 1024 * 1024l;
        //Integer[] memories = new Integer[] { 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 2048 * 1024,
        //        4096 * 1024, 8192 * 1024 };

        Integer[] memories = new Integer[] { 512 * 1024 };
        for (Integer mem : memories) {
            partitionedConfigs.add(new Config(new MemoryConfig(8192 * 4, mem, 8192, 10, true), disk,
                    new TuningConfig(MAX_PAGES, PAGE_SIZE, 1, 10), CARD, HybridSelector.INSTANCE,
                    GreedySelector.INSTANCE, log, log));
        }

        KeyGenerator[] keyGens = new KeyGenerator[] { new UniformGenerator() };

        List<Map<Integer, String>> maps = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        //LSMSimulatorHorizontal.ROUND_ROBIN = true;
        for (KeyGenerator keyGen : keyGens) {
            parallelSimulations(partitionedConfigs, keyGen, "partitioned", memories, maps, futures, false);
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