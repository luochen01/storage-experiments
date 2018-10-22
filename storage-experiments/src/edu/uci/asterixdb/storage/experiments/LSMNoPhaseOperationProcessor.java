package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class LSMNoPhaseOperationProcessor {

    private static class OperationStat {
        public double inputRecords;

        public double pages;
        // ms
        public double time;

    }

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/operation/limit_level_operation_nophase.log";
    private final String outputPath = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/operation/no-phase-avg-";

    public static void main(String[] args) throws IOException {
        new LSMNoPhaseOperationProcessor().run();;
    }

    public void run() throws IOException {
        parse(new File(basePath));
    }

    private void parse(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        Map<Integer, Boolean> isFinished = new HashMap<>();
        Map<Integer, PrintWriter> writers = new HashMap<>();
        Map<Integer, OperationStat> stats = new HashMap<>();

        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.contains("Merge 2.0 components")) {
                int level = getLevel(line);
                isFinished.put(level, true);
                PrintWriter writer = writers.get(level);
                if (writer == null) {
                    writer = initializeWriter(level);
                    writers.put(level, writer);
                }
                OperationStat stat = stats.remove(level);
                writer.println(stat.inputRecords + "\t" + stat.pages + "\t" + stat.time + "\t" + 0);
            } else if (line.contains("AbstractLSMProcessingUnit - storage/partition_0")) {
                double records = getRecords(line);
                double pages = getPages(line);
                double time = getTime(line);
                int operations = getOperations(line);
                int level = getUnitLevel(line);
                OperationStat stat = stats.computeIfAbsent(level, k -> new OperationStat());
                stat.inputRecords += records;
                stat.pages += pages;
                stat.time += time;
            }
        }

        for (PrintWriter writer : writers.values()) {
            writer.close();
        }

        reader.close();

    }

    private PrintWriter initializeWriter(int level) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(outputPath + level));
        writer.println("records\tpages\ttime\toperations");
        return writer;
    }

    private String getValue(String line, String del, String endDel) {
        int index = line.indexOf(del);
        int endIndex = endDel != null ? line.indexOf(endDel, index) : line.length();
        return line.substring(index + del.length(), endIndex);
    }

    private int getUnitLevel(String line) {
        return Integer.valueOf(getValue(line, "kv-merge-", ":"));

    }

    private int getLevel(String line) {
        return Integer.valueOf(getValue(line, "in level ", " takes"));
    }

    private double getRecords(String line) {
        return Double.valueOf(getValue(line, "input records ", ","));
    }

    private double getPages(String line) {
        return Double.valueOf(getValue(line, "pages ", ","));
    }

    private double getTime(String line) {
        return Double.valueOf(getValue(line, "time ", " ms"));
    }

    private int getOperations(String line) {
        return Integer.valueOf(getValue(line, "operations ", null));
    }
}
