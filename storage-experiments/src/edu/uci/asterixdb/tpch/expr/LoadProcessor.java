package edu.uci.asterixdb.tpch.expr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadProcessor {

    public static List<Integer> process(String path, int records) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));

        String line = null;
        List<Integer> list = new ArrayList<>();
        while ((line = reader.readLine()) != null && list.size() < records) {
            if (line.contains("records of LineItem")) {
                int start = line.indexOf("Loading speed ") + "Loading speed ".length();
                int end = line.indexOf(" records/s");
                list.add(Integer.valueOf(line.substring(start, end)));
            }
        }
        reader.close();
        return list;
    }

    public static void main(String[] args) throws IOException {
        String dir = "/Users/luochen/Documents/Research/experiments/results/rebalance/load/";
        String[] files = { "load-master-4.log", "load-opt-4-0.5.log", "load-static-4.log" };
        List<List<Integer>> result = new ArrayList<>();

        int records = 600;
        for (String file : files) {
            result.add(process(dir + file, records));
        }
        System.out.println("time\thashing\tdynamic bucketing\tstatic bucketing");
        for (int i = 0; i < records; i++) {
            System.out.println(String.format("%d\t%d\t%d\t%d", i, result.get(0).get(i), result.get(1).get(i),
                    result.get(2).get(i)));
        }

    }

}
