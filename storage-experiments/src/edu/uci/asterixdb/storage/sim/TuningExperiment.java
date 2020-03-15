package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TuningExperiment extends Experiment {
    private static int cardinality = 100 * 1000 * 1000;
    private static int operations = 2 * cardinality;
    private static int tuningCycle = 2 * 1024 * 1024;

    public static void main(String[] args) throws IOException {
        executor.setCorePoolSize(4);
        runTune();

        executor.shutdown();
    }

    private static void runTune() throws IOException {
        File file = new File("lsm_single");
        if (!file.exists()) {
            Simulator sim = new Simulator(createLSMTree(1024 * 1024, false));
            sim.load(file);
        }

        KeyGenerator writeGen = new ScrambleZipfGenerator();
        KeyGenerator readGen = writeGen;

        List<ExperimentSet> experiments = new ArrayList<>();
        Workload workload = new Workload(operations, new LSMWorkload[] { new LSMWorkload(1, 4, writeGen, readGen) });
        SimulateWorkload simWorkload = new SimulateWorkload("write memory", file, workload);

        Config config = createLSMTree(128 * 1024, true);
        experiments.add(parallelSimulations(Collections.singletonList(config), simWorkload, new Integer[] { 0 }, true));
        experiments.forEach(t -> t.await());
    }

    private static void runSearch() throws IOException {
        Integer[] memories =
                new Integer[] { 512 * 1024, 1024 * 1024, 1536 * 1024, 2048 * 1024, 3072 * 1024, 4096 * 1024 };

        File file = new File("lsm_single");
        if (!file.exists()) {
            Simulator sim = new Simulator(createLSMTree(1024 * 1024, false));
            sim.load(file);
        }

        KeyGenerator writeGen = new ScrambleZipfGenerator();
        KeyGenerator readGen = writeGen;

        List<ExperimentSet> experiments = new ArrayList<>();
        Workload workload = new Workload(operations, new LSMWorkload[] { new LSMWorkload(1, 1, writeGen, readGen) });
        SimulateWorkload simWorkload = new SimulateWorkload("write memory", file, workload);

        List<Config> configs = new ArrayList<>();

        for (Integer memory : memories) {
            Config config = createLSMTree(memory, false);
            configs.add(config);
        }
        experiments.add(parallelSimulations(configs, simWorkload, memories, true));
        experiments.forEach(t -> t.await());

        ExperimentSet experiment = experiments.get(0);
        for (int i = 0; i < memories.length; i++) {
            ExperimentResult result = experiment.results.get(i);
            double readCost = (double) result.getDiskReads() * 16 / operations;
            double writeCost = (double) result.getDiskWrites() * 16 / operations;
            System.out.println(
                    String.format("%d\t%.3f\t%.3f\t%.3f", memories[i], readCost, writeCost, readCost + writeCost));
        }

    }

    private static Config createLSMTree(int writeMemory, boolean tune) {
        int totalMemory = 8 * 1024 * 1024;
        int cardinality = 100 * 1000 * 1000;
        MemoryConfig memoryConfig = new MemoryConfig(8 * 1024, 10, true);
        DiskConfig diskConfig = new DiskConfig(10, 0);
        LSMConfig lsmConfig = new LSMConfig(memoryConfig, diskConfig, cardinality);
        TuningConfig tuningConfig = new TuningConfig(writeMemory, totalMemory - writeMemory, 128 * 1024, 16, 14, 1, 1,
                tuningCycle, 32 * 1024, tune);
        Config config = new Config(new LSMConfig[] { lsmConfig }, tuningConfig, 8192, 8192, 10 * 1024 * 1024, 0.5);
        return config;
    }

    private static void runDouble(boolean opt) throws IOException {
        int cardinality = 10 * 1024 * 1024;
        MemoryConfig memoryConfig = new MemoryConfig(8 * 1024, 10, true);
        DiskConfig diskConfig = new DiskConfig(10, 0);
        LSMConfig lsmConfig1 = new LSMConfig(memoryConfig, diskConfig, cardinality / 20);
        LSMConfig lsmConfig2 = new LSMConfig(memoryConfig, diskConfig, cardinality / 20 * 19);
        TuningConfig tuningConfig =
                new TuningConfig(32 * 1024, 992 * 1024, 32 * 1024, 4, 4, 1, 1, 1024 * 1024, 32 * 1024, true);
        Integer[] params = new Integer[] { 0 };
        // use two identical LSM-trees
        Config config =
                new Config(new LSMConfig[] { lsmConfig1, lsmConfig2 }, tuningConfig, 8192, 8192, 1024 * 1024, 0.0);

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