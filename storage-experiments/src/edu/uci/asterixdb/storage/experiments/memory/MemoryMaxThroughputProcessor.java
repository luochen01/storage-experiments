package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MemoryMaxThroughputProcessor {

    private static final int interval = 300;

    private static final String BASE_PATH = "/Users/luochen/Documents/Research/experiments/results/memory/write/single";

    private static final String DYNAMIC_PATH =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/single-dynamic";

    private static final String FLUSH_PATH =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/single-flush";

    private static final String L0_PATH = "/Users/luochen/Documents/Research/experiments/results/memory/write/L0";

    private static final String MEMORY_PATH =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/memory-new";

    //private final String basePath = "/tmp";
    private final String path;
    private final String name;
    private final String pattern;
    private final String[] workloads;
    private final int[] memories;

    public MemoryMaxThroughputProcessor(String path, String name, String pattern, String[] workloads) {
        this(path, name, pattern, workloads, LSMMemoryUtils.memories);
    }

    public MemoryMaxThroughputProcessor(String path, String name, String pattern, String[] workloads, int[] memories) {
        this.path = path;
        this.name = name;
        this.pattern = pattern;
        this.workloads = workloads;
        this.memories = memories;
    }

    public void run() throws IOException {
        int[][] results = new int[workloads.length][];
        for (int i = 0; i < workloads.length; i++) {
            results[i] = runWorkload(workloads[i]);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("\t");
        for (String workload : workloads) {
            sb.append(workload);
            sb.append("\t");
        }
        System.out.println(sb.toString());

        for (int i = 0; i < memories.length; i++) {
            sb = new StringBuilder();
            sb.append(memories[i]);
            sb.append("\t");
            for (int j = 0; j < workloads.length; j++) {
                sb.append(results[j][i]);
                sb.append("\t");
            }
            System.out.println(sb.toString());
        }

    }

    public void runMemory() throws IOException {
        File file = LSMMemoryUtils.getFile(path, pattern);
        System.out.println(pattern);
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.newFormat('\t').withFirstRecordAsHeader());

        Iterator<CSVRecord> iterator = parser.iterator();
        int total = 0;
        int counter = 0;
        long throughput = 0;
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            throughput += Integer.valueOf(record.get(1));
            counter++;
            total++;
            if (counter == interval) {
                System.out.println(total + "\t" + throughput / counter);
                counter = 0;
                throughput = 0;
            }
        }
        if (counter != 0) {
            System.out.println(total + "\t" + throughput / counter);
        }

        parser.close();

    }

    private int[] runWorkload(String workload) throws IOException {
        boolean computeMemoryThroughput = memories != LSMMemoryUtils.memories;
        int[] results = new int[memories.length];
        for (int i = 0; i < memories.length; i++) {
            try {
                File file = LSMMemoryUtils.getFile(path, pattern + "-" + workload + "-mem" + "-" + memories[i]);
                results[i] = computeMemoryThroughput ? computeMemoryThroughput(file)
                        : LSMMemoryUtils.computeThroughput(file);
            } catch (Exception e) {
                results[i] = 0;
            }
        }
        return results;
    }

    private int computeMemoryThroughput(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line = null;
        int total = 0;
        int count = 0;
        int skip = 300;
        int valid = 0;
        boolean start = false;
        while ((line = reader.readLine()) != null) {
            if (line.contains("counter")) {
                start = true;
            } else if (start) {
                count++;
                if (count >= skip) {
                    valid++;
                    String[] parts = line.split("\t");
                    total += Integer.valueOf(parts[1]) + Integer.valueOf(parts[3]);
                }
            }
        }
        reader.close();
        return total / valid;
    }

    public static void main(String[] args) throws IOException {
        //mainSingle();
        //mainFlush();
        //mainL0();
        mainMemory();
    }

    private static void mainSingle() throws IOException {
        new MemoryMaxThroughputProcessor(BASE_PATH, "btree-static-default", "static-8", LSMMemoryUtils.workloads).run();
        new MemoryMaxThroughputProcessor(BASE_PATH, "btree-static-tuned", "static-1", LSMMemoryUtils.workloads).run();
        new MemoryMaxThroughputProcessor(BASE_PATH, "btree-dynamic", "btree", LSMMemoryUtils.workloads).run();
        new MemoryMaxThroughputProcessor(BASE_PATH, "accordion-data", "full-data", LSMMemoryUtils.workloads).run();
        new MemoryMaxThroughputProcessor(BASE_PATH, "accordion-index", "full-index", LSMMemoryUtils.workloads).run();
        new MemoryMaxThroughputProcessor(BASE_PATH, "multi-level", "partition", LSMMemoryUtils.workloads).run();
    }

    private static void mainFlush() throws IOException {
        String[] workloads = { "write" };
        new MemoryMaxThroughputProcessor(FLUSH_PATH, "adaptive", "adaptive", workloads).run();
        new MemoryMaxThroughputProcessor(FLUSH_PATH, "memory", "roundrobin", workloads).run();
        new MemoryMaxThroughputProcessor(FLUSH_PATH, "log", "minlsn", workloads).run();
        new MemoryMaxThroughputProcessor(FLUSH_PATH, "full", "full", workloads).run();
    }

    private static void mainL0() throws IOException {
        String[] workloads = { "write" };
        new MemoryMaxThroughputProcessor(L0_PATH, "default", "single", workloads).run();
        new MemoryMaxThroughputProcessor(L0_PATH, "group", "simple-", workloads).run();
        new MemoryMaxThroughputProcessor(L0_PATH, "group+push", "-push-", workloads).run();
        new MemoryMaxThroughputProcessor(L0_PATH, "group+push+greedy", "pushgreedy", workloads).run();
    }

    private static void mainDynamic() throws IOException {
        new MemoryMaxThroughputProcessor(DYNAMIC_PATH, "", "write-0-1536", LSMMemoryUtils.workloads).runMemory();
        new MemoryMaxThroughputProcessor(DYNAMIC_PATH, "", "write-1536-1536", LSMMemoryUtils.workloads).runMemory();
        new MemoryMaxThroughputProcessor(DYNAMIC_PATH, "", "write-32-1536", LSMMemoryUtils.workloads).runMemory();
    }

    private static void mainMemory() throws IOException {
        new MemoryMaxThroughputProcessor(MEMORY_PATH, "btree", "btree", LSMMemoryUtils.workloads, new int[] { 1, 8 })
                .run();
        new MemoryMaxThroughputProcessor(MEMORY_PATH, "partition", "partition", LSMMemoryUtils.workloads,
                new int[] { 1, 8 }).run();
    }

}
