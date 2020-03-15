package edu.uci.asterixdb.storage.experiments.memory;

import static edu.uci.asterixdb.storage.experiments.memory.LSMMemoryUtils.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MemoryMaxThroughputProcessor {

    private static final int interval = 300;

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/memory/write/single";

    private final String dynamicPath =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/single-dynamic";

    //private final String basePath = "/tmp";

    private final String name;
    private final String pattern;

    public MemoryMaxThroughputProcessor(String name, String pattern) {
        this.name = name;
        this.pattern = pattern;
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
        File file = LSMMemoryUtils.getFile(dynamicPath, pattern);
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
        int[] results = new int[memories.length];
        for (int i = 0; i < memories.length; i++) {
            try {
                results[i] = computeThroughput(
                        LSMMemoryUtils.getFile(basePath, pattern + "-" + workload + "-" + memories[i]));
            } catch (Exception e) {
                results[i] = 0;
            }
        }
        return results;

    }

    public static void main(String[] args) throws IOException {
        mainSingle();
    }

    private static void mainSingle() throws IOException {
        new MemoryMaxThroughputProcessor("btree-static-default", "static-8").run();
        new MemoryMaxThroughputProcessor("btree-static-tuned", "static-1").run();
        new MemoryMaxThroughputProcessor("btree-dynamic", "btree").run();
        new MemoryMaxThroughputProcessor("accordion-data", "full-data").run();
        new MemoryMaxThroughputProcessor("accordion-index", "full-index").run();
        new MemoryMaxThroughputProcessor("multi-level", "partition").run();
    }

    private static void mainDynamic() throws IOException {
        new MemoryMaxThroughputProcessor("", "write-0-1536").runMemory();
        new MemoryMaxThroughputProcessor("", "write-1536-1536").runMemory();
        new MemoryMaxThroughputProcessor("", "write-32-1536").runMemory();
    }

}
