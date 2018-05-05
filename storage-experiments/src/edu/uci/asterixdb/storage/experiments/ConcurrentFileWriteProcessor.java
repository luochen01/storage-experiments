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

    private static final String inputDir =
            "/Users/luochen/Documents/Research/experiments/results/disk io/concurrent write";

    public static void main(String[] args) throws Exception {
        File dir = new File(inputDir);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("filewrite") && !name.contains(".csv");
            }
        });

        String writerToken = "writer-";
        String pageToken = "pages in ";
        String dashToken = " - ";
        String writingToken = "Writing ";
        String bytesToken = " bytes";
        String takesToken = " takes ";
        DecimalFormat formatter = new DecimalFormat("#.00");

        List<Pair<String, String>> throughputs = new ArrayList<>();
        for (File file : files) {
            System.out.println("Processing " + file);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            List<Pair<Integer, Integer>>[] lists = new List[5];
            for (int i = 0; i < lists.length; i++) {
                lists[i] = new ArrayList<>();
            }
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.contains("Writing")) {
                    int index = line.indexOf(writingToken) + writingToken.length();
                    long totalBytes = Long.valueOf(line.substring(index, line.indexOf(bytesToken)));
                    long totalMBs = totalBytes / 1024 / 1024;
                    index = line.indexOf(takesToken) + takesToken.length();
                    long time = Long.valueOf(line.substring(index, line.indexOf(" ms"))) / 1000;
                    double throughput = (double) totalMBs / time;
                    throughputs.add(Pair.of(file.getName(), formatter.format(throughput)));
                } else if (line.contains("Finished")) {
                    int index = line.indexOf(writerToken) + writerToken.length();
                    int level = Integer.valueOf(line.substring(index, index + 1));
                    index = line.indexOf(pageToken) + pageToken.length();
                    int duration = Integer.valueOf(line.substring(index, line.indexOf(" ms", index)));
                    index = line.indexOf(dashToken) + dashToken.length();
                    int time = Integer.valueOf(line.substring(index, line.indexOf(":", index))) / 1000;
                    lists[level].add(Pair.of(time, duration));
                }
            }
            reader.close();
            PrintWriter writer = new PrintWriter(new File(inputDir, file.getName() + ".csv"));
            writer.println(getHeader());
            int count = 0;
            while (true) {
                boolean nonEmpty = false;
                StringBuilder sb = new StringBuilder();
                for (List<Pair<Integer, Integer>> list : lists) {
                    if (list.size() > count) {
                        Pair<Integer, Integer> p = list.get(count);
                        sb.append(p.getLeft());
                        sb.append("\t");
                        sb.append(p.getRight());
                        sb.append("\t");
                        nonEmpty = true;
                    } else {
                        sb.append(" \t \t");
                    }
                }
                if (!nonEmpty) {
                    break;
                }
                writer.println(sb.toString());
                count++;
            }
            writer.close();
        }

        throughputs.sort(new Comparator<Pair<String, String>>() {
            @Override
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                int cmp = getPrefix(o1.getLeft()).compareTo(getPrefix(o2.getLeft()));
                if (cmp == 0) {
                    cmp = Integer.compare(getSuffix(o1.getLeft()), getSuffix(o2.getLeft()));
                }
                return cmp;
            }
        });

        PrintWriter writer = new PrintWriter(new File(inputDir, "result.csv"));
        writer.println("experiment\tthroughput");
        for (Pair<String, String> p : throughputs) {
            writer.println(p.getLeft() + "\t" + p.getRight());
        }
        writer.close();
    }

    private static String getPrefix(String file) {
        String prefix = file.split("\\.")[0];
        if (!prefix.contains("_")) {
            return prefix;
        } else {
            return prefix.substring(0, prefix.lastIndexOf("_"));
        }
    }

    public static int getSuffix(String file) {
        String suffix = file.split("\\.")[0];
        if (!suffix.contains("_")) {
            return 0;
        } else {
            String[] parts = suffix.split("_");
            return Integer.valueOf(parts[parts.length - 1]);
        }
    }

    private static String getHeader() {
        return "time\t32MB\ttime\t64MB\ttime\t128MB\ttime\t256MB\ttime\t512MB";
    }

}
