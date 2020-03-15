package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TPCCProcessor {
    private static final String basePath500 =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/tpcc-500";
    private static final String basePath2000 =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/tpcc-2000";

    private static final String[] Partition_Policies = { "maxmemory", "minlsn", "adaptive" };
    private static final String[] Btree_policies = { "maxmemory" };
    private static final String Workload = "tpcc";

    private final String basePath;
    private final String impl;
    private final String[] policies;

    public TPCCProcessor(String basePath, String impl, String[] policies) {
        this.basePath = basePath;
        this.impl = impl;
        this.policies = policies;
    }

    private void runMaxThroughput() {
        try {
            System.out.println(impl);
            System.out.println(LSMMemoryUtils.toHeader("memory", policies, impl));

            for (int memory : LSMMemoryUtils.memories) {
                Integer[] values = new Integer[policies.length];
                int i = 0;
                for (String policy : policies) {
                    try {
                        File file = LSMMemoryUtils.getFile(basePath, impl, Workload, memory, policy, "");
                        values[i++] = LSMMemoryUtils.computeTPCCThroughput(file);
                    } catch (Exception e) {
                        values[i++] = 0;
                    }
                }
                System.out.println(LSMMemoryUtils.toString(memory, values, "\t"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runTransactionWrite() throws IOException {
        System.out.println(impl);
        System.out.println(LSMMemoryUtils.toHeader("memory", policies, impl));

        for (int memory : LSMMemoryUtils.memories) {
            Integer[] values = new Integer[policies.length];
            int i = 0;
            for (String policy : policies) {
                try {
                    File file = LSMMemoryUtils.getFile(basePath, impl, Workload, memory, policy, "");
                    int throughput = LSMMemoryUtils.computeTPCCThroughput(file);
                    int diskWrite = LSMMemoryUtils.computeTPCCDiskWrite(file);
                    values[i++] = diskWrite * 1024 / Math.max(throughput, 1);
                } catch (Exception e) {
                    values[i++] = 0;
                }

            }
            System.out.println(LSMMemoryUtils.toString(memory, values, "\t"));
        }
    }

    Integer[] memories = { 1024, 2048 };
    String[] tags = { "8-4", "8-10", "16-4", "16-10", "32-4", "32-10" };

    private void runTestThroguhput() throws IOException {
        System.out.println(impl);
        System.out.println(LSMMemoryUtils.toHeader("memory", tags, "partition"));

        for (int memory : memories) {
            Integer[] values = new Integer[tags.length];
            int i = 0;
            for (String tag : tags) {
                File file = LSMMemoryUtils.getFile(basePath, impl, Workload, memory, "adaptive", tag + "-");
                values[i++] = LSMMemoryUtils.computeTPCCThroughput(file);
            }
            System.out.println(LSMMemoryUtils.toString(memory, values, "\t"));
        }
    }

    private void runTotalDiskWrites() {
        try {
            System.out.println(impl);
            System.out.println(LSMMemoryUtils.toHeader("meomry", policies, impl));
            for (int memory : LSMMemoryUtils.memories) {
                Integer[] values = new Integer[policies.length];
                int i = 0;
                for (String policy : policies) {
                    File file = LSMMemoryUtils.getLogFile(basePath, impl, policy, memory, Workload, ".log");
                    values[i++] = computeTotalWrites(file) / 1024;
                }
                System.out.println(LSMMemoryUtils.toString(memory, values, "\t"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runTestTotalWrites() throws IOException {
        System.out.println(impl);
        System.out.println(LSMMemoryUtils.toHeader("memory", tags, "partition"));

        for (int memory : memories) {
            Integer[] values = new Integer[tags.length];
            int i = 0;
            for (String tag : tags) {
                File file = LSMMemoryUtils.getLogFile(basePath, impl, "adaptive", memory, Workload, tag, ".log");
                values[i++] = computeTotalWrites(file) / 1024;
            }
            System.out.println(LSMMemoryUtils.toString(memory, values, "\t"));
        }
    }

    private int computeTotalWrites(File file) throws IOException {
        if (file == null) {
            return 0;
        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String targetLine = null;
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.contains("[memory allocation]")) {
                targetLine = line;
            }
        }
        double totalWriteMBs = 0;
        String[] parts = targetLine.split(",");
        for (int i = 1; i < parts.length; i++) {
            String[] subs = parts[i].split("/");
            totalWriteMBs += LSMMemoryUtils.parseWriteMBs(subs[1]);
            totalWriteMBs += LSMMemoryUtils.parseWriteMBs(subs[2]);
        }
        reader.close();
        return (int) totalWriteMBs;
    }

    public static void main(String[] args) throws IOException {
        new TPCCProcessor(basePath500, "static", Btree_policies).runMaxThroughput();
        new TPCCProcessor(basePath500, "static", Btree_policies).runTransactionWrite();
        new TPCCProcessor(basePath500, "btree", Btree_policies).runMaxThroughput();
        new TPCCProcessor(basePath500, "btree", Btree_policies).runTransactionWrite();
        new TPCCProcessor(basePath500, "partition", Partition_Policies).runMaxThroughput();
        new TPCCProcessor(basePath500, "partition", Partition_Policies).runTransactionWrite();

        new TPCCProcessor(basePath2000, "static", Btree_policies).runMaxThroughput();
        new TPCCProcessor(basePath2000, "static", Btree_policies).runTransactionWrite();

        new TPCCProcessor(basePath2000, "btree", Btree_policies).runMaxThroughput();
        new TPCCProcessor(basePath2000, "btree", Btree_policies).runTransactionWrite();

        new TPCCProcessor(basePath2000, "partition", Partition_Policies).runMaxThroughput();
        new TPCCProcessor(basePath2000, "partition", Partition_Policies).runTransactionWrite();

        //        new TPCCProcessor(basePath1000Test, "partition", null).runTestThroguhput();
        //        new TPCCProcessor(basePath1000Test, "partition", null).runTestTotalWrites();

    }

}
