package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

public class FlowControlMergeProcessor {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/prefix";

    //private final String basePath = "/tmp";

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(".log") && name.contains(".log");
            }
        });
        for (File file : files) {
            process(file);
        }
    }

    private void process(File file) throws IOException {
        File dir = new File(basePath, "merges");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        String str = "Schedule merging components ";

        int totalComponents = 0;
        int totalMerges = 0;

        int[] merges = new int[11];
        while ((line = reader.readLine()) != null) {
            if (line.contains(str)) {
                int index = line.indexOf(str) + str.length();
                String[] parts = line.substring(index).split(", ");
                merges[parts.length]++;
                totalMerges++;

            }
        }

        reader.close();

        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dir, file.getName())));
        writer.write("components\tratio\tmerges\ttotal merges\n");
        for (int i = 2; i <= 10; i++) {
            writer.write(i + "\t" + (double) merges[i] / totalMerges + "\t" + merges[i] + "\t" + totalMerges + "\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        new FlowControlMergeProcessor().run();

    }

}
