package edu.uci.asterixdb.storage.sim;

class TuningExperiment extends Experiment {
    public static final int PAGE_SIZE = 4;
    public static final int SIMULATE_CACHE_SIZE = 64 * 1024;
    public static final int MIN_MEMORY_SIZE = 32 * 1024;

    // 10M keys
    private static long log = 1 * 1024 * 1024l;
    //long log = Integer.MAX_VALUE;
    private static int memory = 1 * 1024 * 1024; // 1G
    private static int simulateSize = (int) (memory * 0.001);

}