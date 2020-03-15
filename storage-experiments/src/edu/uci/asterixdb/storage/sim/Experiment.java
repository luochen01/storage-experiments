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
import java.util.function.Function;

class ExperimentSet {
    final String name;
    Map<Integer, ExperimentResult> results = Collections.synchronizedMap(new HashMap<>());
    List<Future<?>> futures = new ArrayList<>();

    public ExperimentSet(String name) {
        this.name = name;
    }

    public void await() {
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

class ExperimentResult {
    Object param;
    String memoryAmp;
    String diskAmp;
    String averageDiskFlushes;
    int maxDiskFlushes;
    long mergeDiskReads;
    long queryDiskReads;
    long diskFlushWrites;
    long diskMergeWrites;
    int fullFlushes;
    long mergeReads;
    long queryReads;

    long savedMergeDiskReads;
    long savedQueryDiskReads;

    public long getReads() {
        return mergeReads + queryReads;
    }

    public long getDiskReads() {
        return mergeDiskReads + queryDiskReads;
    }

    public long getSavedDiskReads() {
        return savedMergeDiskReads + savedQueryDiskReads;
    }

    public double getCacheMissRatio() {
        return (double) getDiskReads() / getReads();
    }

    public long getDiskWrites() {
        return diskMergeWrites + diskFlushWrites;
    }
}

class Experiment {
    public static final MemoryConfig MEMORY_CONFIG = new MemoryConfig(4 * 1024, 10, true);
    public static final DiskConfig DISK = new DiskConfig(10, 0);
    public static final int CARDINALITY = 10 * 1024 * 1024;
    public static final LSMConfig LSM_CONFIG = new LSMConfig(MEMORY_CONFIG, DISK, CARDINALITY);
    public static final TuningConfig TUNING_CONFIG =
            new TuningConfig(512 * 1024, 512 * 1024, 32 * 1024, 4, 4, 1, 1, 1024 * 1024, 32 * 1024, true);
    public static final Config CONF = new Config(new LSMConfig[] { LSM_CONFIG }, null, 8192, 8192, 1024 * 1024, 0.0);

    public static int NUM_THREADS = 4;
    public static final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    public static int SIM_FACTOR = 1;

    public static void loadSimulation(Config config, File file) throws IOException {
        Simulator sim = new Simulator(config);
        sim.load(file);
        System.out.println("Completed data loading");
    }

    public static ExperimentSet parallelSimulations(List<Config> configs, SimulateWorkload workload, Object[] params,
            boolean collectTuningLog) {
        ExperimentSet set = new ExperimentSet(workload.name);
        for (int i = 0; i < configs.size(); i++) {
            Config config = configs.get(i);
            final Object param = params[i];
            final int index = i;
            SimulateWorkload newWorkload = workload.clone();
            Future<?> future = executor.submit(() -> {
                System.out.println(String.format("starting %s-%s", newWorkload.name, param));
                Simulator sim = new Simulator(config);
                if (collectTuningLog) {
                    try {
                        sim.initializeTuningLog(new File(newWorkload.name + "-" + param.toString()
                                + (config.tuningConfig.enabled ? "-tune" : "") + ".log"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    sim.simulate(newWorkload);
                    SimulationStats stats = sim.stats;
                    ExperimentResult result = new ExperimentResult();
                    result.param = param;
                    result.memoryAmp = sim.printWriteAmplification(stats.memoryMergeKeys);
                    result.diskAmp = sim.printWriteAmplification(stats.diskMergeKeys);
                    result.averageDiskFlushes = sim.printAverageFlushesPerLogTruncation();
                    result.maxDiskFlushes = stats.maxDiskFlushesPerLogTruncation;
                    result.mergeDiskReads = sim.cache.getMergeDiskReads();
                    result.queryDiskReads = sim.cache.getQueryDiskReads();
                    result.diskFlushWrites = sim.cache.getFlushDiskWrites();
                    result.diskMergeWrites = sim.cache.getMergeDiskWrites();
                    result.savedMergeDiskReads = sim.cache.getSavedMergeDiskReads();
                    result.savedQueryDiskReads = sim.cache.getSavedQueryDiskReads();
                    result.mergeReads = sim.cache.getMergeReads();
                    result.queryReads = sim.cache.getQueryReads();
                    result.fullFlushes = sim.stats.fullFlushes;
                    set.results.put(index, result);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            set.futures.add(future);
        }
        return set;
    }

    public static void printExperiments(List<ExperimentSet> list, String header,
            Function<ExperimentResult, String> processor) {
        list.forEach(set -> set.await());

        for (ExperimentSet set : list) {
            System.out.println(set.name);
            System.out.println(header);
            for (int i = 0; i < set.results.size(); i++) {
                System.out.println(processor.apply(set.results.get(i)));
            }
        }
    }

}