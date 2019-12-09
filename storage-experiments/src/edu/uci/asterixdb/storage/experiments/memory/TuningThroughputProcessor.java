package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class TuningThroughputProcessor {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/memory/tune/";

    //private final String basePath = "/tmp";

    private static final int writeIndex = 1;
    private static final int readIndex = 3;

    public TuningThroughputProcessor() {
    }

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".log");
            }
        });
        for (File file : files) {
            process(file);
        }

    }

    private static final NumberFormat formatter = new DecimalFormat("#0.00");

    private void process(File file) throws IOException {
        PrintWriter writer = new PrintWriter(new File(file.getAbsolutePath() + ".csv"));

        int start = 0;
        int count = 0;
        long totalOps = 0;
        long memory = 0;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;

        writer.println(String.format("%s\t%s\t%s", "time", "throughput", "memory"));
        while ((line = reader.readLine()) != null) {
            int ops = getOps(line);
            if (ops >= 0) {
                totalOps += ops;
                count++;
            }
            if (line.contains("AsterixMemoryTuner - currentLSN")) {
                if (memory == 0) {
                    int index = getIndex(line, 0, "memory component size: ");
                    int endIndex = line.indexOf("->", index);
                    memory = parseSizeMB(line.substring(index, endIndex));
                }

                writer.println(String.format("%d\t%d\t%d", start, totalOps / count, memory));
                start += count;
                count = 0;
                totalOps = 0;
                int index = getIndex(line, 0, "->");
                int endIndex = line.indexOf(",", index);
                memory = parseSizeMB(line.substring(index, endIndex));
            }
        }
        reader.close();

        writer.println(String.format("%d\t%d\t%d", start, totalOps / count, memory));
        writer.close();
        System.out.println("processed " + file.getName());
    }

    private int getOps(String line) {
        try {
            String[] parts = line.split("\t");
            if (parts.length >= 9) {
                return Integer.valueOf(parts[writeIndex]) + Integer.valueOf(parts[readIndex]);
            }
        } catch (Exception e) {
        }

        return -1;
    }

    public static void main(String[] args) throws IOException {
        new TuningThroughputProcessor().run();
    }

    private static int getIndex(String line, int start, String pattern) {
        int index = line.indexOf(pattern, start);
        if (index < 0) {
            return index;
        } else {
            return index + pattern.length();
        }
    }

    private static int parseSizeMB(String size) {
        double value = Double.valueOf(size.substring(0, size.length() - 2));
        if (size.endsWith("GB")) {
            value *= 1024;
        }
        return (int) value;
    }

}
