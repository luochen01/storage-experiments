package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class FlowControlMaxThroughputProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/partition";

    //private final String basePath = "/tmp";

    private final int skip = 60;

    private static final Set<Integer> validSizes = new HashSet<>();
    {
        validSizes.add(2048);
        validSizes.add(16384);
        validSizes.add(131072);
        validSizes.add(1048576);
        validSizes.add(8388608);
    }

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return validSizes.contains(Integer.valueOf(getSize(name))) && !name.contains(".log");
            }
        });
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return Integer.compare(getSize(o1.getName()), getSize(o2.getName()));
            }
        });
        for (File file : files) {
            process(file);
        }
    }

    private static final NumberFormat formatter = new DecimalFormat("#0.00");

    private void process(File file) throws IOException {

        int size = getSize(file.getName());

        BufferedReader csvReader = new BufferedReader(new FileReader(file));
        csvReader.readLine();

        int count = 0;
        int validConut = 0;
        String line = null;
        long totalSize = 0;
        while ((line = csvReader.readLine()) != null) {
            count++;
            if (count >= skip) {
                totalSize += Integer.valueOf(line.split("\t")[1]);
                validConut++;
            }
        }
        csvReader.close();
        System.out.println(size + "\t" + totalSize / validConut);

    }

    private static int getSize(String file) {
        try {
            return Integer.valueOf(file.split("-")[3].split("\\.")[0]);
        } catch (Exception e) {
            return 0;
        }
    }

    public static void main(String[] args) throws IOException {
        new FlowControlMaxThroughputProcessor().run();
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
