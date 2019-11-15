package edu.uci.asterixdb.storage.experiments.memory;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MemoryMaxThroughputProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/active-component-no-wait";

    //private final String basePath = "/tmp";

    private final int skip = 600;

    private final String pattern;

    public MemoryMaxThroughputProcessor(String pattern) {
        this.pattern = pattern;
    }

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return !name.contains(".log") && name.contains(pattern);
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

    private static final NumberFormat formatter = new DecimalFormat("#0.00");

    private void process(File file) throws IOException {

        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.newFormat('\t').withFirstRecordAsHeader());
        int count = 0;
        int validConut = 0;
        long totalSize = 0;
        long maxThroughput = 0;
        for (CSVRecord record : parser) {
            maxThroughput = Math.max(Integer.valueOf(record.get(1)), maxThroughput);
            count++;
            if (count > skip) {
                validConut++;
                totalSize += Integer.valueOf(record.get(1));
            }
        }
        parser.close();
        System.out.println(getSize(file.getName()) + "\t" + totalSize / Math.max(1, validConut) + "\t" + maxThroughput);

    }

    private static int getSize(String file) {
        try {
            return Integer.valueOf(file.split("-")[3]);
        } catch (Exception e) {
            return 0;
        }
    }

    public static void main(String[] args) throws IOException {
        new MemoryMaxThroughputProcessor("partition").run();
        new MemoryMaxThroughputProcessor("full").run();

    }

}
