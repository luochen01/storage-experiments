package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlowControlReadAggProcessor {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/point";

    private final String suffix = ".read";

    private final int bucketSize = 1000;
    private final int bucketPeriod = 1;

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

        BufferedReader reader = new BufferedReader(new FileReader(file));
        PrintWriter readWriter =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".read.txt")));

        readWriter.println("counter\ttime(mean)\ttime(99th)\ttime(co std)");

        String line = null;

        List<Double> list = new ArrayList<>();
        System.out.println("processing " + file);
        while ((line = reader.readLine()) != null) {
            list.add(Double.valueOf(line) / 1000);
        }

        Collections.sort(list);

        System.out.println("avg: " + avg(list));

        System.out.println("95th: " + list.get((int) (list.size() * 0.95)));
        System.out.println("99th: " + list.get((int) (list.size() * 0.99)));
        System.out.println("99.9th: " + list.get((int) (list.size() * 0.999)));

        readWriter.close();
        reader.close();

    }

    private double avg(List<Double> list) {
        double total = 0;
        for (double i : list) {
            total += i;
        }
        return total / list.size();
    }

    private double std(List<Double> list) {
        double avg = avg(list);
        double sum = 0;
        for (double i : list) {
            sum += (i - avg) * (i - avg);
        }
        return Math.sqrt(sum / list.size());
    }

    public static void main(String[] args) throws IOException {
        new FlowControlReadAggProcessor().run();
    }

}
