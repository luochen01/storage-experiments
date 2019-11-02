package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;

import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;

public class FlowControlReadLatencyProcessor {

    private final String basePath;

    //private final String basePath = "/tmp";

    private final String suffix = ".read";

    private final int bucketPeriod = 60;

    public FlowControlReadLatencyProcessor(String basePath) {
        this.basePath = basePath;
    }

    public void run() throws IOException, InterruptedException {

        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(suffix) && name.contains("long");
            }
        });
        Thread[] threads = new Thread[files.length];
        for (int i = 0; i < files.length; i++) {
            final File file = files[i];
            threads[i] = new Thread(() -> {
                try {
                    process(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private double getPercentile(File file) {
        if (file.getName().contains(".read")) {
            return 0.9999;
        } else if (file.getName().contains(".scan")) {
            return 0.999;
        } else if (file.getName().contains(".long")) {
            return 0.99;
        } else {
            throw new IllegalStateException("Unknown file name " + file.getName());
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
        double percentile = getPercentile(file);
        int topK = Math.max(1, bucketSize - (int) ((double) bucketSize * percentile));
        double sum = 0;
        int max = 0;
        try {
            while (true) {
                time = reader.readInt();
                sum += time;
                if (queue.size() <= topK || time > queue.firstInt()) {
                    queue.enqueue(time);
                    if (queue.size() > topK) {
                        queue.dequeueInt();
                    }
                    max = Math.max(max, time);
                }
                index++;
                if (index == bucketSize) {
                    count++;

                    int value = 0;
                    value = queue.dequeueInt();

                    readWriter.println(String.format("%d\t%.3f\t%.3f\t%.3f", count * bucketPeriod,
                            (double) sum / bucketSize / 1000, (double) value / 1000, (double) max / 1000));
                    bucketSize = getBucketSize(csvReader);
                    index = 0;
                    sum = 0;
                    topK = bucketSize - (int) ((double) bucketSize * percentile);
                    queue.clear();
                    max = 0;
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

    public static void main(String[] args) throws IOException, InterruptedException {
        String level = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/level-query";
        String tier = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/tier-query";

        new FlowControlReadLatencyProcessor(level).run();
        new FlowControlReadLatencyProcessor(tier).run();
    }

}
