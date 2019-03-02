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

import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;

public class FlowControlReadLatencyProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/secondary-query";

    //private final String basePath = "/tmp";

    private final String suffix = ".read";

    private final int bucketPeriod = 30;

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
        File derived = new File(basePath + "/latency");
        if (!derived.exists()) {
            derived.mkdir();
        }
        System.out.println("Processing " + file.getName());
        DataInputStream reader = new DataInputStream(new FileInputStream(file));

        BufferedReader csvReader =
                new BufferedReader(new FileReader(file.getAbsolutePath().replaceAll(".read.csv", "")));
        csvReader.readLine();
        csvReader.readLine();

        PrintWriter readWriter =
                new PrintWriter(new FileWriter(new File(basePath + "/latency/" + file.getName() + ".latency.txt")));

        readWriter.println("counter\ttime(mean)\ttime(99.9th)\ttime(max)");

        int time = 0;
        int index = 0;
        int count = 0;
        int bucketSize = getBucketSize(csvReader);

        IntHeapPriorityQueue queue = new IntHeapPriorityQueue();
        int topK = Math.max(1, bucketSize - (int) ((double) bucketSize * 0.999));
        double sum = 0;
        try {
            while (true) {
                time = reader.readInt();
                sum += time;
                if (queue.size() <= topK || time > queue.firstInt()) {
                    queue.enqueue(time);
                    if (queue.size() > topK) {
                        queue.dequeueInt();
                    }
                }
                index++;
                if (index == bucketSize) {
                    count++;

                    int percentile = 0;
                    percentile = queue.dequeueInt();
                    int max = percentile;
                    while (!queue.isEmpty()) {
                        max = queue.dequeueInt();
                    }

                    readWriter.println(String.format("%d\t%.3f\t%.3f\t%.3f", count * bucketPeriod,
                            (double) sum / bucketSize / 1000, (double) percentile / 1000, (double) max / 1000));
                    bucketSize = getBucketSize(csvReader);
                    index = 0;
                    sum = 0;
                    topK = bucketSize - (int) ((double) bucketSize * 0.999);
                    queue.clear();
                    if (count * bucketPeriod % 300 == 0) {
                        System.out.println(count * bucketPeriod);
                        readWriter.flush();
                    }
                }

            }
        } catch (Exception e) {
            readWriter.close();
            csvReader.close();
            reader.close();
            e.printStackTrace();
        }
    }

    private int getBucketSize(BufferedReader reader) throws IOException {
        int total = 0;
        for (int i = 0; i < bucketPeriod; i++) {
            String line = reader.readLine();
            total += Integer.valueOf(line.split("\t")[3]);
        }
        return total;
    }

    private double percentile(int[] array, int len) {
        return array[(int) (len * 0.999)];
    }

    private double avg(int[] array, int len) {
        double total = 0;
        for (int i = 0; i < len; i++) {
            total += array[i];
        }
        return total / len;
    }

    private double std(int[] array, int len) {
        double avg = avg(array, len) / 1000;
        double sum = 0;
        for (int i = 0; i < len; i++) {
            double val = (double) array[i] / 1000;
            sum += (val - avg) * (val - avg);
        }
        return Math.sqrt(sum / len);
    }

    public static void main(String[] args) throws IOException {
        new FlowControlReadLatencyProcessor().run();
        //        int cycles = 0;
        //        boolean high = false;
        //        long total = 0;
        //        for (int i = 0; i < 3600 * 3; i++) {
        //            cycles++;
        //            if (high) {
        //                total += 25000;
        //                if (cycles == 600) {
        //                    cycles = 0;
        //                    high = false;
        //                }
        //            } else {
        //                total += 6000;
        //                if (cycles == 600 * 5) {
        //                    cycles = 0;
        //                    high = true;
        //                }
        //            }
        //            System.out.println(total);
        //        }

    }

}
