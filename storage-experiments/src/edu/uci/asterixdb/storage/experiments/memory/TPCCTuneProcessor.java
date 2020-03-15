package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class TPCCTuneProcessor {

    private static interface LineParser {
        int parseOperations(CSVRecord record);

        int parseDiskWrites(CSVRecord record);

        int parseDiskReads(CSVRecord record);

    }

    private static final String tpccBasePathTune =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/tpcc-2000-tune/";

    private static final String tpccBasePathTuneChange =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/tpcc-2000-tune-change/";

    private static final String tpccBasePathTuneChangeRatio =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/tpcc-2000-tune-change-ratio/";

    private static final String ycsbBasePathTune =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/ycsb-tune/";

    private static final LineParser tpccParser = new LineParser() {
        @Override
        public int parseOperations(CSVRecord record) {
            return Integer.valueOf(record.get(1).split(",")[0]);
        }

        @Override
        public int parseDiskWrites(CSVRecord record) {
            int writes = Integer.valueOf(record.get(9).split(",")[0]);
            return Math.max(writes, 0);
        }

        @Override
        public int parseDiskReads(CSVRecord record) {
            int reads = Integer.valueOf(record.get(9).split(",")[1]);
            return Math.max(reads, 0);
        }
    };

    private static final LineParser ycsbParser = new LineParser() {
        @Override
        public int parseOperations(CSVRecord record) {
            return Integer.valueOf(record.get(1)) + Integer.valueOf(record.get(3));
        }

        @Override
        public int parseDiskWrites(CSVRecord record) {
            return Math.max(Integer.valueOf(record.get(5)), 0);

        }

        @Override
        public int parseDiskReads(CSVRecord record) {
            return Math.max(Integer.valueOf(record.get(6)), 0);
        }
    };

    private static final int interval = 600;
    private static final int totalTime = 3600 * 4;

    private final String path;
    private final String pattern;
    private final String name;
    private final LineParser parser;

    public TPCCTuneProcessor(String path, String pattern, String name, LineParser parser) {
        this.path = path;
        this.pattern = pattern;
        this.name = name;
        this.parser = parser;
    }

    private void run() throws IOException {
        File file = LSMMemoryUtils.getFile(path, pattern);
        System.out.println(name);
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.newFormat('\t').withFirstRecordAsHeader());
        int count = 0;
        long totalOps = 0;
        long totalReads = 0;
        long totalWrites = 0;
        int time = 0;

        for (CSVRecord record : parser) {
            time++;
            count++;
            totalOps += this.parser.parseOperations(record);
            totalWrites += this.parser.parseDiskWrites(record);
            totalReads += this.parser.parseDiskReads(record);
            if (count >= interval) {
                System.out.println(String.format("%d\t%d\t%.2f\t%d\t%.2f\t%d\t%.2f", time, totalOps / count,
                        (double) totalWrites * 1024 / totalOps, totalWrites / count,
                        (double) totalReads * 1024 / totalOps, totalReads / count,
                        (double) (totalWrites + totalReads) * 1024 / totalOps));
                count = 0;
                totalOps = 0;
                totalWrites = 0;
                totalReads = 0;
            }
            if (time >= totalTime) {
                break;
            }
        }
        parser.close();
    }

    private void runMemory() throws IOException {
        File file = LSMMemoryUtils.getLogFile(path, pattern, ".log");
        System.out.println(name);
        System.out.println(0 + "\t" + 64);

        String line = null;
        Date begin = null;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        int writeMemory = 0;
        double cost = 0;
        try {
            while ((line = reader.readLine()) != null) {
                if (begin == null && line.contains("ERROR")) {
                    String str = line.split(" ")[0];
                    begin = LSMMemoryUtils.parseDate(str);
                } else if (line.contains("delta")) {
                    Date date = LSMMemoryUtils.parseDate(line.split(" ")[0]);
                    if (date.compareTo(begin) < 0) {
                        date.setDate(date.getDate() + 1);
                    }
                    long duration = (date.getTime() - begin.getTime()) / 1000;
                    if (duration >= totalTime) {
                        break;
                    }
                    String pattern = "->";
                    int index = line.indexOf(pattern) + pattern.length();
                    int end = line.indexOf(", ", index);
                    String sub = line.substring(index, end);
                    writeMemory = (int) LSMMemoryUtils.parseWriteMBs(sub);

                    pattern = "total cost: ";
                    index = line.indexOf(pattern) + pattern.length();
                    index = line.indexOf("/", index) + 1;
                    end = line.indexOf(",", index);
                    cost = Double.valueOf(line.substring(index, end));
                    System.out.println(duration + "\t" + writeMemory + "\t" + cost);
                }
            }
        } catch (Exception e) {
            System.out.println(line);
            e.printStackTrace();
        }
        System.out.println(totalTime + "\t" + writeMemory + "\t" + cost);

        reader.close();
    }

    public static void main(String[] args) throws IOException {
        //mainYCSB();
        //mainTPCCChange();
        mainTPCCChangeRatio("12288");
    }

    private static void mainYCSB() throws IOException {
        String[] memories = { "4096", "20480" };
        String[] memoryNames = { "4G", "20G" };
        String[] workloads = { "write-10-read", "write-20-read", "write-30-read", "write-40-read", "write-50-read" };
        String[] workloadNames = { "10% writes", "20% writes", "30% writes", "40% writes", "50% writes" };

        for (int i = 0; i < memories.length; i++) {
            for (int j = 0; j < workloads.length; j++) {
                new TPCCTuneProcessor(ycsbBasePathTune, "write-partition-tune-" + workloads[j] + "-" + memories[i],
                        memoryNames[i] + "-" + workloadNames[j], ycsbParser).runMemory();
            }
        }
    }

    private static void mainTPCCChange() throws IOException {
        String[] memories = { "4096", "8192", "12288", "16384", "20480" };
        String[] names = { "4G", "8G", "12G", "16G", "20G" };
        for (int i = 0; i < memories.length; i++) {
            new TPCCTuneProcessor(tpccBasePathTuneChange, "tpcc-partition-2000-tune-change-" + memories[i], names[i],
                    tpccParser).runMemory();
        }
    }

    private static void mainTPCCChangeRatio(String memory) throws IOException {
        String[] names = { "0.1", "0.3", "0.5", "1.0" };
        for (int i = 0; i < names.length; i++) {
            new TPCCTuneProcessor(tpccBasePathTuneChangeRatio,
                    "tpcc-partition-2000-tune-change-" + memory + "-" + names[i], names[i], tpccParser).runMemory();
        }
    }

    private static void mainTPCC() throws IOException {
        String[] memories = { "4096", "8192", "12288", "16384", "20480" };
        String[] names = { "4G", "8G", "12G", "16G", "20G" };
        for (int i = 0; i < memories.length; i++) {
            new TPCCTuneProcessor(tpccBasePathTune, "tpcc-partition-2000-tune-" + memories[i], names[i], tpccParser)
                    .runMemory();
        }
    }

}
