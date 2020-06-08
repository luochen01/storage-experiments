package edu.uci.asterixdb.storage.experiments.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TraceReader {

    public static void main(String[] args) throws IOException {
        int[] sizes = new int[4096 * 2 / 10 + 1];

        String path = "/Users/luochen/Desktop/btree/tpcc-btree-run.trace";

        BufferedReader reader = new BufferedReader(new FileReader(path));

        String line = null;
        int total = 0;

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            if (parts.length == 3) {
                int size = Integer.valueOf(parts[parts.length - 1]) / 10;
                sizes[size]++;
                total++;
            }
        }
        reader.close();

        double sum = 0;
        for (int i = 0; i < sizes.length; i++) {
            sum += (double) sizes[i] / total * 100;
            System.out.println(i * 10 + "\t" + sum);
        }

    }
}