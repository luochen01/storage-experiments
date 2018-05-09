package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Experiment performance with sequential and random I/Os
 *
 * @author luochen
 *
 */
public class FileSkipReadProcessor {

    private static String[] sels = { "1", "0.95", "0.8", "0.5", "0.25", "0.1", "0.01" };
    static String path = "/Users/luochen/Documents/Research/experiments/results/disk io/read/";

    public static void main(String[] args) throws IOException {
        parseFiles("sel_", ".log");
        parseFiles("sel_", "_sleep_1.log");
        parseFiles("sel_", "_sleep_2.log");
        parseFiles("sel_", "_sleep_5.log");

    }

    private static void parseFiles(String prefix, String suffix) throws IOException {
        System.out.println(suffix);
        for (String sel : sels) {
            String filename = prefix + sel + suffix;
            File file = new File(path + filename);

            BufferedReader reader = new BufferedReader(new FileReader(file));

            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.contains("Read") && line.contains("ms")) {
                    String[] parts = line.split(" ");
                    long time = Long.valueOf(parts[4]);
                    System.out.println(sel + "\t" + time);
                }
            }
            reader.close();

        }

        System.out.println();

    }

}
