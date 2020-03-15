package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TPCCTestProcessor {
    private static final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/tpcc-2000-tune";

    private static final int interval = 300;

    private final String path;
    private final String pattern;
    private final String name;

    public TPCCTestProcessor(String path, String pattern, String name) {
        this.path = path;
        this.pattern = pattern;
        this.name = name;
    }

    private void run() throws IOException {
        File file = LSMMemoryUtils.getLogFile(path, pattern, "tpcc", ".log", 2000);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        int count = 0;
        String dataset = "stock:";
        while ((line = reader.readLine()) != null) {
            if (line.contains("[memory allocation]")) {
                int begin = line.indexOf(dataset) + dataset.length();
                int end = line.indexOf(',', begin);
                String[] parts = line.substring(begin, end).split("/");
                double overlap = Double.valueOf(parts[4]);
                count++;
                System.out.println(count + "\t" + overlap);
            }
        }
        reader.close();
    }

    public static void main(String[] args) throws IOException {
        new TPCCTestProcessor(basePath, "tpcc-partition-2000-tune-4096", "4096").run();
    }

}
