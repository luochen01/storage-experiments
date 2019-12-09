package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;

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
        runSingle();
    }

    private static void runSingle() throws IOException {
        int cardinality = 10 * 1024 * 1024;
        MemoryConfig memoryConfig = new MemoryConfig(8 * 1024, 10, true);
        DiskConfig diskConfig = new DiskConfig(10, 0);
        LSMConfig lsmConfig = new LSMConfig(memoryConfig, diskConfig, cardinality);
        TuningConfig tuningConfig =
                new TuningConfig(32 * 1024, 992 * 1024, 32 * 1024, 4, 1, 1, 1024 * 1024, 32 * 1024, true);
        Config config = new Config(new LSMConfig[] { lsmConfig }, tuningConfig, 8192, 8192, 1024 * 1024);

        File file = new File("lsm_single");
        if (!file.exists()) {
            Simulator sim = new Simulator(config);
            sim.load(file);
        }
        Simulator sim = new Simulator(config);

        KeyGenerator writeGen = new ScrambleZipfGenerator();
        KeyGenerator readGen = new ScrambleZipfGenerator();
        Workload workload = new Workload(cardinality, new LSMWorkload(1, 1, writeGen, readGen));
        SimulateWorkload simWorkload = new SimulateWorkload("lsm_single", file, workload);
        sim.simulate(simWorkload);

    }

}