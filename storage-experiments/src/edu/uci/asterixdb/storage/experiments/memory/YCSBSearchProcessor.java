package edu.uci.asterixdb.storage.experiments.memory;

import java.io.File;
import java.io.IOException;

public class YCSBSearchProcessor {

    private static class Result {
        String memory;
        int throguhput;
        double diskRead;
        double diskWrite;

        public Result(String memory, int throguhput, double diskRead, double diskWrite) {
            this.memory = memory;
            this.throguhput = throguhput;
            this.diskRead = diskRead;
            this.diskWrite = diskWrite;
        }

        public double getDiskIo() {
            return diskRead + diskWrite;
        }

    }

    private static final String ycsbTune =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/ycsb-tune/";

    private static final String ycsbSearch =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/ycsb-search/";
    private static final int[] totalMemories = { 4096, 8192, 20480 };

    private static final int[] memories4G = { 128, 2048, 256, 512, 1024, 1536, 3072 };
    private static final int[] memories8G = { 128, 4096, 512, 1024, 1536, 2048, 3072 };
    private static final int[] memories20G = { 128, 10240, 512, 1024, 2048, 3072, 4096 };

    private static final int tuneSkip = 2400;
    private static final int searchSkip = 1200;

    private static final String[] workloads =
            { "write-10-read", "write-20-read", "write-30-read", "write-40-read", "write-50-read" };
    private static final String[] workloadNames = { "10%-write", "20%-write", "30%-write", "40%-write", "50%-write" };

    private final String pattern;

    private final int totalMemory;
    private final int[] searchMemories;

    public YCSBSearchProcessor(String pattern, int totalMemory, int[] searchMemories) {
        this.pattern = pattern;
        this.totalMemory = totalMemory;
        this.searchMemories = searchMemories;
    }

    private void runSearch() throws IOException {
        System.out.println(totalMemory);
        String header = "memory\tthroughput\tdisk I/O";
        System.out.println("workload\t" + header + "\t" + header + "\t" + header + "\t" + header);
        int i = 0;
        for (String workload : workloads) {
            Result tunedResult =
                    parseResult(ycsbTune, pattern + "-tune-" + workload + "-" + totalMemory, "tuned", tuneSkip);
            Result defaultResult = parseResult(ycsbSearch,
                    pattern + "-" + workload + "-" + totalMemory + "-" + searchMemories[0] + "-", searchMemories[0],
                    searchSkip);
            Result halfResult = parseResult(ycsbSearch,
                    pattern + "-" + workload + "-" + totalMemory + "-" + searchMemories[1] + "-", searchMemories[1],
                    searchSkip);

            Result bestResult = defaultResult.getDiskIo() < halfResult.getDiskIo() ? defaultResult : halfResult;

            for (int j = 0; j < searchMemories.length; j++) {
                Result result = parseResult(ycsbSearch,
                        pattern + "-" + workload + "-" + totalMemory + "-" + searchMemories[j] + "-", searchMemories[j],
                        searchSkip);
                System.out.println(printResult(result));
                if (result.getDiskIo() < bestResult.getDiskIo()) {
                    bestResult = result;
                }
            }
            System.out.println(workloadNames[i++] + "\t" + printResult(tunedResult) + "\t" + printResult(bestResult)
                    + "\t" + printResult(defaultResult) + "\t" + printResult(halfResult));
        }
    }

    private Result parseResult(String path, String pattern, Object memory, int skip) throws IOException {
        try {
            File file = LSMMemoryUtils.getFile(path, pattern);
            int diskRead =
                    LSMMemoryUtils.doComputeThroughput(file, skip, record -> Integer.valueOf(record.get(6))) * 1024;
            int diskWrite =
                    LSMMemoryUtils.doComputeThroughput(file, skip, record -> Integer.valueOf(record.get(5))) * 1024;
            int throughput = LSMMemoryUtils.doComputeThroughput(file, skip,
                    record -> Integer.valueOf(record.get(1)) + Integer.valueOf(record.get(3)));
            return new Result(memory.toString(), throughput, (double) diskWrite / throughput,
                    (double) diskRead / throughput);
        } catch (Exception e) {
            return new Result("0", 0, 1000, 1000);
        }

    }

    private String printResult(Result result) {
        return String.format("%s\t%d\t%.2f\t%.2f\t%.2f", result.memory, result.throguhput, result.diskWrite,
                result.diskRead, result.diskRead + result.diskWrite);
    }

    public static void main(String[] args) throws IOException {
        new YCSBSearchProcessor("write-partition", 20480, memories20G).runSearch();
        new YCSBSearchProcessor("write-partition", 8192, memories8G).runSearch();
        new YCSBSearchProcessor("write-partition", 4096, memories4G).runSearch();

        //        new TPCCProcessor(basePath1000Test, "partition", null).runTestThroguhput();
        //        new TPCCProcessor(basePath1000Test, "partition", null).runTestTotalWrites();

    }

    private static void tpccMain() throws IOException {

    }

}
