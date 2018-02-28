package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Predicate;

class MergeStat {
    // in ms
    long time;
    // in byte
    long size;
    long primaryKeys;
    long secondaryKeys;

    public MergeStat(long time, long size, long primaryKeys, long secondaryKeys) {
        this.time = time;
        this.size = size;
        this.primaryKeys = primaryKeys;
        this.secondaryKeys = secondaryKeys;
    }

    //in kb
    public long formatSize() {
        return size / 1024;
    }

    // in s
    public double formatTime() {
        return (double) time / 1000000;
    }
}

class Experiment {
    String name;
    List<MergeStat> stats;

    public Experiment(String name, List<MergeStat> stats) {
        super();
        this.name = name;
        this.stats = stats;
    }
}

public class TracingProcessor {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/ingest/repair/repair-cmp";

    private static final String deliminiter = "Read Ahead Limit for Merge";
    private static final String PRIMARY = "#Primary keys during merge repair ";
    private static final String SECONDARY = "#Secondary Index Elements during merge repair ";

    private final Predicate<String> isBegin = new Predicate<String>() {
        @Override
        public boolean test(String t) {
            return t.contains("sid_idx") && t.contains("\"ph\":\"B\"");
        }

    };

    private final Predicate<String> isEnd = new Predicate<String>() {
        @Override
        public boolean test(String t) {
            return t.contains("sid_idx") && t.contains("\"ph\":\"E\"");
        }
    };

    Queue<Integer> primaryKeys = new LinkedList<>();
    Queue<Integer> secondaryKeys = new LinkedList<>();

    private List<Experiment> parse(File file) throws IOException {
        List<Experiment> result = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        List<MergeStat> stats = new ArrayList<>();
        Map<Long, Long> beginTimes = new HashMap<>();
        int seqNum = 1;
        while ((line = reader.readLine()) != null) {
            if (line.contains(deliminiter)) {
                if (!stats.isEmpty()) {
                    result.add(new Experiment(file.getName().replaceAll(".log", "") + "-" + seqNum, stats));
                    stats = new ArrayList<>();
                    seqNum++;
                    beginTimes.clear();
                }
                primaryKeys.clear();
                secondaryKeys.clear();
            }
            if (isBegin.test(line)) {
                parseKeys(line);
                long tid = parseTid(line);
                if (beginTimes.containsKey(tid)) {
                    //  throw new IllegalStateException();
                }
                beginTimes.put(tid, parseTime(line));
            } else if (isEnd.test(line)) {
                parseKeys(line);
                long tid = parseTid(line);
                Long begin = beginTimes.remove(tid);
                if (begin == null) {
                    //throw new IllegalStateException();
                    continue;
                }
                long end = parseTime(line);
                long size = parseSize(line);
                MergeStat stat = new MergeStat(end - begin, size, primaryKeys.poll(), secondaryKeys.poll());
                stats.add(stat);
            } else {
                parseKeys(line);
            }
        }
        if (!stats.isEmpty()) {
            result.add(new Experiment(file.getName().replaceAll(".log", "") + "-" + seqNum, stats));
        }

        reader.close();
        return result;
    }

    private void parseKeys(String line) {
        if (line.contains(PRIMARY)) {
            primaryKeys.add(Integer
                    .valueOf(line.substring(PRIMARY.length(), line.indexOf("K", PRIMARY.length() - PRIMARY.length()))));
        } else if (line.contains(SECONDARY)) {
            secondaryKeys.add(Integer.valueOf(
                    line.substring(SECONDARY.length(), line.indexOf("K", SECONDARY.length() - SECONDARY.length()))));
        }
    }

    private void print(Experiment expr) throws IOException {
        PrintWriter writer = new PrintWriter(new File(basePath, expr.name));
        writer.println("size\ttime\tskeys\tpkeys");

        for (MergeStat stat : expr.stats) {
            writer.print(stat.formatSize());
            writer.print('\t');
            writer.print(stat.formatTime());
            writer.print('\t');
            writer.print(stat.secondaryKeys);
            writer.print('\t');
            writer.println(stat.primaryKeys);
        }

        writer.close();
    }

    public void run() {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith("ingest_r_alg.log");
            }
        });
        for (File file : files) {
            try {
                List<Experiment> exprs = parse(file);
                for (Experiment expr : exprs) {
                    print(expr);
                }
                System.out.println("Processed " + file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] agrs) {
        new TracingProcessor().run();
    }

    private long parseTime(String line) {
        String ts = "\"ts\":";
        int index = line.indexOf(ts) + ts.length();
        int endIndex = index + 1;
        while (Character.isDigit(line.charAt(endIndex))) {
            endIndex++;
        }
        System.out.println(line);
        return Long.valueOf(line.substring(index, endIndex));
    }

    private long parseTid(String line) {
        String tid = "\"tid\":";
        int index = line.indexOf(tid) + tid.length();
        int endIndex = line.indexOf(',', index);
        return Long.valueOf(line.substring(index, endIndex));
    }

    private long parseSize(String line) {
        String size = "\"size\":";
        int index = line.indexOf(size) + size.length();
        int endIndex = line.indexOf('}', index);
        return Long.valueOf(line.substring(index, endIndex));
    }

}
