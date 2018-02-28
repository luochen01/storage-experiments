package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MergeKeysProcessor {

    private static final String PRIMARY = "#Primary keys during merge repair ";
    private static final String SECONDARY = "#Secondary Index Elements during merge repair ";

    public static void main(String[] args) throws NumberFormatException, IOException {
        String path = "/Users/luochen/Desktop/log-correlated";

        File file = new File(path);

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        int primaryKeys = 0;
        int secondaryKeys = 0;
        while ((line = reader.readLine()) != null) {
            if (line.contains(PRIMARY)) {
                primaryKeys = Integer.valueOf(
                        line.substring(PRIMARY.length(), line.indexOf("K", PRIMARY.length() - PRIMARY.length())));
            } else if (line.contains(SECONDARY)) {
                secondaryKeys = Integer.valueOf(
                        line.substring(SECONDARY.length(), line.indexOf("K", SECONDARY.length() - SECONDARY.length())));
                System.out.println(secondaryKeys + "\t" + primaryKeys);
                secondaryKeys = 0;
                primaryKeys = 0;
            }

        }
        reader.close();

    }

}
