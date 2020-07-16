package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TPCCPlotProcessor {
    private static final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/memory/tune/tpcc-plot";
    private static final int[] memories_8G =
            { 64, 128, 256, 512, 768, 1024, 1280, 1536, 2048, 3072, 4096, 6144, 7168, 7680 };

    private static final int[] memories_2G = { 64, 128, 256, 512, 768, 1024, 1280, 1536, 1664, 1792 };

    private final String pattern;
    private final int[] memories;

    public TPCCPlotProcessor(String pattern, int[] memories) {
        this.pattern = pattern;
        this.memories = memories;
    }

    public void runPlot() throws IOException {
        System.out.println("memory\twrite cost\tmerge read cost\tquery read cost\ttotal cost");
        for (int memory : memories) {
            File file = LSMMemoryUtils.getLogFile(basePath, pattern + "-" + memory + ".log");
            parseFile(memory, file);
        }

    }

    private void parseFile(int memory, File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;

        double queryReadCost = 0;
        double mergeReadCost = 0;
        double writeCost = 0;

        String mergeReadPattern = "merge disk reads/txn: ";

        String readCostPattern = "read cost: ";
        String writeCostPattern = "write cost: ";

        while ((line = reader.readLine()) != null) {
            if (line.contains(mergeReadPattern)) {
                mergeReadCost = Double.valueOf(line.substring(mergeReadPattern.length()));
            } else if (line.contains(readCostPattern) && line.contains(writeCostPattern)) {
                double readCost = Double.valueOf(line.substring(readCostPattern.length(), line.indexOf(",")));

                int index = line.indexOf(writeCostPattern);
                writeCost = Double.valueOf(line.substring(index + writeCostPattern.length(), line.indexOf(',', index)));
                queryReadCost = readCost - mergeReadCost;
            }
        }

        reader.close();

        System.out.println(String.format("%d\t%.3f\t%.3f\t%.3f\t%.3f", memory, writeCost, mergeReadCost, queryReadCost,
                writeCost + mergeReadCost + queryReadCost));

    }

    public static void main(String[] args) throws IOException {
        new TPCCPlotProcessor("10-2048", memories_2G).runPlot();
        //new TPCCPlotProcessor("10-8192", memories_8G).runPlot();;

    }

}
