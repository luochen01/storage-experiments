package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TuningExperiment extends Experiment {
    public static final int PAGE_SIZE = 4;
    public static final int SIMULATE_CACHE_SIZE = 64 * 1024;
    public static final int MIN_MEMORY_SIZE = 32 * 1024;

    // 10M keys
    private static long log = 1 * 1024 * 1024l;
    //long log = Integer.MAX_VALUE;
    private static int memory = 1 * 1024 * 1024; // 1G
    private static int simulateSize = (int) (memory * 0.001);

    public static void main(String[] args) throws IOException {
        executor.setCorePoolSize(4);
        runDouble(true);
        runDouble(false);

        executor.shutdown();
    }

    private static void runDouble(boolean opt) throws IOException {
        MemoryTuner.OPT_MULTI_LSM = opt;
        int cardinality = 10 * 1024 * 1024;
        MemoryConfig memoryConfig = new MemoryConfig(8 * 1024, 10, true);
        DiskConfig diskConfig = new DiskConfig(10, 0);
        LSMConfig lsmConfig1 = new LSMConfig(memoryConfig, diskConfig, cardinality / 20);
        LSMConfig lsmConfig2 = new LSMConfig(memoryConfig, diskConfig, cardinality / 20 * 19);
        TuningConfig tuningConfig =
                new TuningConfig(32 * 1024, 992 * 1024, 32 * 1024, 4, 1, 1, 1024 * 1024, 32 * 1024, true);
        Integer[] params = new Integer[] { 0 };
        // use two identical LSM-trees
        Config config = new Config(new LSMConfig[] { lsmConfig1, lsmConfig2 }, tuningConfig, 8192, 8192, 1024 * 1024);

        File file = new File("lsm_double");
        if (!file.exists()) {
            Simulator sim = new Simulator(config);
            sim.load(file);
        }

        KeyGenerator writeGen = new ScrambleZipfGenerator();
        KeyGenerator readGen = new ScrambleZipfGenerator();

        int writes = 4;
        Integer[] skewOps = new Integer[] { 1, 4, 19 };
        //Integer[] skewOps = new Integer[] { 19 };

        List<ExperimentSet> experiments = new ArrayList<>();

        for (int skew : skewOps) {
            Workload workload = new Workload(cardinality * 2,
                    new LSMWorkload[] { new LSMWorkload(skew * writes, skew, writeGen, readGen),
                            new LSMWorkload(1 * writes, skew, writeGen, readGen) });
            SimulateWorkload simWorkload =
                    new SimulateWorkload("skew-" + skew + (opt ? "-opt" : "-simple"), file, workload);
            experiments.add(parallelSimulations(Collections.singletonList(config), simWorkload, params, true));
        }

        experiments.forEach(t -> t.await());

    }

}