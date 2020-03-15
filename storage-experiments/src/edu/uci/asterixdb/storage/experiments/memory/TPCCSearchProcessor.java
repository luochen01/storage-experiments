package edu.uci.asterixdb.storage.experiments.memory;

import java.io.File;
import java.io.IOException;

public class TPCCSearchProcessor {
    private static final String tpccTunePath =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/tpcc-2000-tune-change/";

    private static final String tpccPath =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/tpcc-2000-search/";
    private static final int[] totalMemories = { 4096, 8192, 12288, 16384, 20480 };

    private static final int[] memories4G = { 1152, 64, 2048 };
    private static final int[] memories8G = { 1152, 64, 4096 };
    private static final int[] memories12G = { 1152, 64, 6144 };
    private static final int[] memories16G = { 1152, 64, 8192 };
    private static final int[] memories20G = { 1152, 64, 10240 };

    private final String path;
    private final String pattern;

    private final int[] memories;

    public TPCCSearchProcessor(String path, String pattern, int[] memories) {
        this.path = path;
        this.pattern = pattern;
        this.memories = memories;
    }

    private void runDiskIO() throws IOException {
        System.out.println(pattern);
        System.out.println("memory\tthroughput\tdisk I/O");
        for (int memory : memories) {
            File file = LSMMemoryUtils.getFile(path, pattern, "-" + memory + "-");
            int diskRead = LSMMemoryUtils.computeTPCCDiskRead(file);
            int diskWrite = LSMMemoryUtils.computeTPCCDiskWrite(file);
            int throughput = LSMMemoryUtils.computeTPCCThroughput(file);
            System.out.println(String.format("%d\t%d\t%.2f\t%.2f\t%.2f", memory, throughput,
                    (double) diskWrite * 1024 / throughput, (double) diskRead * 1024 / throughput,
                    (double) (diskWrite + diskRead) * 1024 / throughput));
        }
    }

    public static void main(String[] args) throws IOException {
        new TPCCSearchProcessor(tpccTunePath, "tpcc-partition-2000", totalMemories).runDiskIO();
        new TPCCSearchProcessor(tpccPath, "tpcc-partition-2000-4096", memories4G).runDiskIO();
        new TPCCSearchProcessor(tpccPath, "tpcc-partition-2000-8192", memories8G).runDiskIO();
        new TPCCSearchProcessor(tpccPath, "tpcc-partition-2000-12288", memories12G).runDiskIO();
        new TPCCSearchProcessor(tpccPath, "tpcc-partition-2000-16384", memories16G).runDiskIO();
        new TPCCSearchProcessor(tpccPath, "tpcc-partition-2000-20480", memories20G).runDiskIO();
        //        new TPCCProcessor(basePath1000Test, "partition", null).runTestThroguhput();
        //        new TPCCProcessor(basePath1000Test, "partition", null).runTestTotalWrites();
    }

    private static void tpccMain() throws IOException {

    }

}
