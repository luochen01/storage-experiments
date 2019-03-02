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

public class FlowControlReadOpsProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/secondary-query";

    //private final String basePath = "/tmp";

    private final String suffix = "write";

    private final int shortBucketSize = 30;

    private final int longBucketSize = 60;

    private final int skip = 1;

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(suffix) && !name.contains(".log") && !name.contains(".read");
            }
        });
        for (File file : files) {
            process(file);
        }
    }

    private static final NumberFormat formatter = new DecimalFormat("#0.00");

    private void process(File file) throws IOException {
        boolean isLong = file.getName().contains("long");

        File derived = new File(basePath + "/derived");
        if (!derived.exists()) {
            derived.mkdir();
        }
        System.out.println("Processing " + file.getName());
        BufferedReader csvReader =
                new BufferedReader(new FileReader(file.getAbsolutePath().replaceAll(".read.csv", "")));

        for (int i = 0; i < skip; i++) {
            csvReader.readLine();
        }

        PrintWriter writer =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".read.txt")));
        writer.println("time\tlookups\twrites\trecords");
        String line = null;
        int totalReads = 0;
        int totalMBs = 0;
        int totalRecords = 0;
        int count = 0;
        int i = 0;
        int bucketSize = isLong ? longBucketSize : shortBucketSize;
        while ((line = csvReader.readLine()) != null) {
            String[] parts = line.split("\t");
            totalReads += Integer.valueOf(parts[3]);
            totalMBs += Integer.valueOf(parts[5]) / 256;
            totalRecords += Integer.valueOf(parts[1]);
            i++;
            if (i == bucketSize) {
                count++;
                if (isLong) {
                    writer.println((count * bucketSize) + "\t" + totalReads + "\t" + totalMBs / bucketSize + "\t"
                            + totalRecords / bucketSize);
                } else {
                    writer.println((count * bucketSize) + "\t" + totalReads / bucketSize + "\t" + totalMBs / bucketSize
                            + "\t" + totalRecords / bucketSize);
                }

                totalReads = 0;
                totalMBs = 0;
                totalRecords = 0;
                i = 0;
            }
        }
        writer.close();
        csvReader.close();
    }

    public static void main(String[] args) throws IOException {
        new FlowControlReadOpsProcessor().run();
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
