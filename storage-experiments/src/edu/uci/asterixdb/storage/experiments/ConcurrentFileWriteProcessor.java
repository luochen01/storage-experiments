package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public class ConcurrentFileWriteProcessor {

    private static final String inputDir = "/Users/luochen/Documents/Research/experiments/results/disk io/ssd write";

    public static void main(String[] args) throws Exception {
        File dir = new File(inputDir);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("writer") && !name.contains(".csv");
            }
        });

        String[] writers = new String[] { "1", "2", "4", "8", "16" };
        String[] forces = new String[] { "4", "8", "16", "32", "64", "no" };

        for (String writer : writers) {
            for (String force : forces) {
                File file = new File(dir, "writer-" + writer + "-force-" + force);
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line = null;
                long time = 0;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("bytes takes")) {
                        time = Long.valueOf(line.split(" ")[4]);
                        break;
                    }
                }
                reader.close();
                long throughput = 128 * 1024 / (time / 1000);
                System.out.print(throughput + " MB/s\t");
            }
            System.out.println();
        }
    }

}
