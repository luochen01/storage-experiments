package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

public class FlowControlSecondaryEagerProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/secondary";

    //private final String basePath = "/tmp";

    private final String suffix = "write";

    private final int shortBucketSize = 30;

    private final int longBucketSize = 60;

    private final int skip = 1;

    private final String[] patterns = new String[] { "eager-greedy-", "eager-" };

    public void run() throws Exception {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(".log");
            }
        });
        Set<File> processed = new HashSet<>();
        for (String pattern : patterns) {
            process(pattern, files, processed);
        }
    }

    private static final NumberFormat formatter = new DecimalFormat("#0.00");

    private void process(String pattern, File[] files, Set<File> processed) throws Exception {
        File derived = new File(basePath + "/secondary");
        if (!derived.exists()) {
            derived.mkdir();
        }
        System.out.println("Processing " + pattern);
        PrintWriter writer = new PrintWriter(new FileWriter(new File(basePath + "/secondary/" + pattern + ".txt")));
        writer.println("write\t99th latency");

        List<Pair<Integer, Double>> list = new ArrayList<>();

        for (File file : files) {
            if (!processed.contains(file) && file.getName().contains(pattern)) {
                String[] names = file.getName().split("-");
                int rate = Integer.valueOf(names[names.length - 1].split("\\.")[0]);

                double latency = getLatency(file);
                list.add(Pair.of(rate, latency));

                processed.add(file);
            }
        }

        list.sort(new Comparator<Pair<Integer, Double>>() {
            @Override
            public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                return -Integer.compare(o1.getKey(), o2.getKey());
            }
        });

        for (Pair<Integer, Double> p : list) {
            writer.println(p.getKey() + "\t" + formatter.format(p.getValue() / 1000 / 1000));
        }

        writer.close();
    }

    private double getLatency(File file) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.contains("[Intended-UPDATE], 99thPercentileLatency(us)")) {
                String[] parts = line.split(" ");
                reader.close();
                return Double.valueOf(parts[parts.length - 1]);

            }
        }
        reader.close();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        new FlowControlSecondaryEagerProcessor().run();

    }

}
