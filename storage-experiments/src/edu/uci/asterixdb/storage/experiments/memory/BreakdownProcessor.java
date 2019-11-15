package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class BreakdownProcessor {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/memory/write";

    //private final String basePath = "/tmp";

    private final String pattern;

    public BreakdownProcessor(String pattern) {
        this.pattern = pattern;
    }

    private static final Set<Integer> validSizes = new HashSet<>();
    {
        validSizes.add(128);
        validSizes.add(256);
        validSizes.add(512);
        validSizes.add(1024);
        validSizes.add(2048);
        validSizes.add(4096);
    }

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return validSizes.contains(getSize(name)) && name.contains(".log") && name.contains(pattern);
            }
        });
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return Integer.compare(getSize(o1.getName()), getSize(o2.getName()));
            }
        });
        System.out.println(pattern);
        for (File file : files) {
            process(file);
        }
    }

    private void process(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        long memoryTime = 0;
        long diskTime = 0;
        while ((line = reader.readLine()) != null) {
            if (line.contains("DISK_MERGE_EXEC") || line.contains("DISK_MERGE_COMP")) {
                diskTime += Long.valueOf(line.split("\t")[3]);
            }
            if (line.contains("MEMORY_MERGE_EXEC") || line.contains("MEMORY_MERGE_COMP")) {
                memoryTime += Long.valueOf(line.split("\t")[3]);
            }
        }
        reader.close();
        System.out.println(getSize(file.getName()) + "\t" + memoryTime / 1000 / 1000 + "\t" + diskTime / 1000 / 1000);
    }

    private static int getSize(String file) {
        try {
            return Integer.valueOf(file.split("-")[2].split("\\.")[0]);
        } catch (Exception e) {
            return 0;
        }
    }

    public static void main(String[] args) throws IOException {
        new BreakdownProcessor("partition").run();
        //        new BreakdownProcessor("full").run();
    }

}
