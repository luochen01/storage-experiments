package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class TweetProcessor {

    public static void main(String[] args) throws IOException {
        String input = "/Users/luochen/Documents/Research/experiments/tweets.adm";
        String output = "/Users/luochen/Documents/Research/experiments/tweets_random.adm";

        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output)));
        String line;

        String id = "int64(\"";
        int count = 0;
        Random random = new Random(50);
        while ((line = reader.readLine()) != null) {
            int startIndex = line.indexOf(id);
            startIndex += id.length();
            int endIndex = line.indexOf('"', startIndex);

            long randId = Math.abs(random.nextLong());

            String newLine = line.replaceFirst(line.substring(startIndex, endIndex), String.valueOf(randId));
            writer.write(newLine);
            writer.write(System.lineSeparator());
            if (count++ % 10000 == 0) {
                System.out.println("Processed " + count + " lines");
            }
        }

        reader.close();
        writer.close();

    }

}
