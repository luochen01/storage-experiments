package edu.uci.asterixdb.storage.experiments.memory;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TuningProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/projects/storage-experiments/storage-experiments";

    //private final String basePath = "/tmp";

    private static final int COST_INDEX = 4;

    private final String pattern;

    public TuningProcessor(String pattern) {
        this.pattern = pattern;
    }

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(pattern) && getSize(name) > 0 && name.endsWith(".log");
            }
        });
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return Integer.compare(getSize(o1.getName()), getSize(o2.getName()));
            }
        });
        List<DoubleList> lists = new ArrayList<>();
        for (File file : files) {
            DoubleList list = process(file);
            lists.add(list);
        }

        for (File file : files) {
            System.out.print(getSize(file.getName()));
            System.out.print("\t");
        }
        System.out.println();

        int index = 0;
        boolean empty = false;
        while (!empty) {
            StringBuilder sb = new StringBuilder();
            for (DoubleList list : lists) {
                if (list.size() <= index) {
                    empty = true;
                    break;
                }
                sb.append(list.getDouble(index));
                sb.append("\t");
            }
            if (!empty) {
                System.out.println(sb.toString());
            }
            index++;
        }

    }

    private static final NumberFormat formatter = new DecimalFormat("#0.00");

    private DoubleList process(File file) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.newFormat('\t'));
        DoubleList list = new DoubleArrayList();
        for (CSVRecord record : parser) {
            list.add(Double.valueOf(record.get(COST_INDEX)));
        }
        parser.close();
        return list;
    }

    private static int getSize(String file) {
        try {
            String[] parts = file.replace(".log", "").split("-");
            return Integer.valueOf(parts[parts.length - 1]);
        } catch (Exception e) {
            return 0;
        }
    }

    public static void main(String[] args) throws IOException {
        new TuningProcessor("scramblezipf-uniform").run();
    }

}
