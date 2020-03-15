package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class WriteExperiment extends Experiment {
    public static final int PAGE_SIZE = 16;

    private static long log = 10 * 1024 * 1024l;
    //long log = Integer.MAX_VALUE;
    private static int card = 100 * 1024 * 1024; // 100G

    public static void main(String[] args) throws IOException {
        executor.setCorePoolSize(4);
        Integer[] memories = { 320 * 1024, 384 * 1024 };
        List<Config> configs = new ArrayList<>();
        for (int i = 0; i < memories.length; i++) {
            configs.add(createPartitionConfig(memories[i]));
        }
        ExperimentSet expr = runSingle(configs, memories);

        for (int i = 0; i < configs.size(); i++) {
            System.out.println(memories[i]);
            System.out.println("memory amp " + expr.results.get(i).memoryAmp);
            System.out.println("disk flush writes " + expr.results.get(i).diskFlushWrites);
            System.out.println("disk merge writes " + expr.results.get(i).diskMergeWrites);
            System.out.println("full flushes " + expr.results.get(i).fullFlushes);
        }

        executor.shutdown();
        Runtime.getRuntime().halt(0);
    }

    private static Config createPartitionConfig(int memory) {
        MemoryConfig memoryConfig = new MemoryConfig(8 * 1024, 10, true);
        DiskConfig diskConfig = new DiskConfig(10, 1);
        LSMConfig lsmConfig = new LSMConfig(memoryConfig, diskConfig, card);
        TuningConfig tuningConfig =
                new TuningConfig(memory, 10 * 1024 * 1024, 0, PAGE_SIZE, PAGE_SIZE, 0, 0, Integer.MAX_VALUE, 0, false);
        Config config = new Config(new LSMConfig[] { lsmConfig }, tuningConfig, 8192, 8192, log, 0.5);
        return config;
    }

    private static void load(Config config) throws IOException {
        File file = new File("lsm_single");
        if (!file.exists()) {
            Simulator sim = new Simulator(config);
            sim.load(file);
        }
    }

    private static ExperimentSet runSingle(List<Config> configs, Integer[] params) throws IOException {

        KeyGenerator writeGen = new RangeSkewGenerator(20);
        KeyGenerator readGen = new RangeSkewGenerator(20);

        File file = new File("lsm_single");

        Workload workload = new Workload(card / 2, new LSMWorkload[] { new LSMWorkload(1, 0, writeGen, readGen) });
        SimulateWorkload simWorkload = new SimulateWorkload("workload", file, workload);
        ExperimentSet result = parallelSimulations(configs, simWorkload, params, false);
        result.await();
        return result;
    }

}