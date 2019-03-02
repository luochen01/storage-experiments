package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class FlowControlFullScanProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/full-scan";

    //private final String basePath = "/tmp";

    private final String suffix = ".read";

    private final int bucketSize = 30;

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(suffix);
            }
        });
        for (File file : files) {
            process(file);
        }
    }

    private void process(File file) throws IOException {
        File derived = new File(basePath + "/derived");
        if (!derived.exists()) {
            derived.mkdir();
        }
        System.out.println("Processing " + file.getName());
        DataInputStream reader = new DataInputStream(new FileInputStream(file));

        BufferedReader csvReader =
                new BufferedReader(new FileReader(file.getAbsolutePath().replaceAll(".read.csv", "")));

        csvReader.readLine();

        PrintWriter writer =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".latency.txt")));

        writer.println("time\tquery\tmax query\tdisk");

        try {
            int count = 0;

            while (true) {
                long totalDiskWrites = 0;
                int queries = 0;
                for (int i = 0; i < bucketSize; i++) {
                    String[] parts = csvReader.readLine().split("\t");
                    queries += Integer.valueOf(parts[3]);
                    totalDiskWrites += Integer.valueOf(parts[5]);
                }

                long queryTime = 0;
                long maxQueryTime = 0;
                for (int i = 0; i < queries; i++) {
                    reader.readLong();
                    int time = reader.readInt();
                    maxQueryTime = Math.max(maxQueryTime, time);
                    queryTime += time;
                }
                count++;
                writer.println(
                        String.format("%d\t%.1f\t%.1f\t%d", count * bucketSize, (double) queryTime / 1000 / 1000 / queries,
                                (double) maxQueryTime / 1000 / 1000, totalDiskWrites / bucketSize / 256));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writer.close();
            reader.close();
            csvReader.close();
        }

    }

    public static void main(String[] args) throws IOException {
        new FlowControlFullScanProcessor().run();

    }

}
