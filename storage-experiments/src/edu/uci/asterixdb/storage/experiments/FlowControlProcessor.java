package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

class Latency {
    double value;
    int count;

    public Latency(double value, int count) {
        this.value = value;
        this.count = count;
    }
}

public class FlowControlProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/flowcontrol/uniform/partition";

    private final String suffix = ".log";

    private final int latencyBins = 99;

    private final int binLength = 20;

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(suffix);
            }
        });
        for (File file : files) {
            process(file);
        }
    }

    private void process(File file) throws IOException {
        File derived = new File(basePath + "/derived");
        if (!derived.exists()) {
            derived.mkdir();
        }

        BufferedReader reader = new BufferedReader(new FileReader(file));
        PrintWriter latencyWriter =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".latency.txt")));
        PrintWriter mergeWriter =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".merge.txt")));
        PrintWriter partitionWriter =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".partition.txt")));

        List<Latency> latencies = new ArrayList<>();
        latencyWriter.println("latency\tcount\tpercentage");
        PrintWriter componentWriter =
                new PrintWriter(new FileWriter(new File(basePath + "/derived/" + file.getName() + ".component.txt")));
        componentWriter.println("time\tcount");
        mergeWriter.println("#components");
        partitionWriter.println("level1\tlevel1 total\tlevel2\tlevel2 total\ttotal");
        String line = null;
        double totalCount = 0;
        long mergeComponents = 0;
        long merges = 0;
        int level1MergeCount = 0;
        int level2MergeCount = 0;
        int level1Merge = 0;
        int level2Merge = 0;

        while ((line = reader.readLine()) != null) {
            if (line.contains("[Intended-UPDATE]")) {
                totalCount += processLatency(line, latencies);
            } else if (line.contains("#components")) {
                processComponents(line, componentWriter);
            } else if (line.contains("Schedule merging components")) {
                int len = line.split(", ").length;
                merges++;
                mergeComponents += len;
                mergeWriter.println(len);
            } else if (line.contains("PartitionedLevelMergePolicy - Schedule merge")) {
                if (line.contains("at level 0+1")) {
                    partitionWriter.println(level1MergeCount + "\t" + level1Merge + "\t" + level2MergeCount + "\t"
                            + level2Merge + "\t" + (level1Merge + level2Merge));
                    level1MergeCount = 0;
                    level2MergeCount = 0;
                    level1Merge = 0;
                    level2Merge = 0;
                } else if (line.contains("at level 1+2")) {
                    level1MergeCount++;
                    level1Merge += parseMerges(line);
                } else if (line.contains("at level 2+3")) {
                    level2MergeCount++;
                    level2Merge += parseMerges(line);
                }
            }
        }
        partitionWriter.close();

        int numResultLatencies = 0;
        Latency active = new Latency(binLength, 0);

        for (Latency latency : latencies) {
            boolean succ = false;
            while (!succ) {
                if (latency.value <= active.value || numResultLatencies == latencyBins) {
                    active.count += latency.count;
                    succ = true;
                } else {
                    numResultLatencies++;
                    latencyWriter.println(String.format("%.0f\t%d\t%.3f", active.value, active.count,
                            active.count / totalCount * 100));
                    active = new Latency((double) (numResultLatencies + 1) * binLength, 0);
                }
            }
        }
        latencyWriter
                .println(String.format("%.0f\t%d\t%.3f", active.value, active.count, active.count / totalCount * 100));

        reader.close();
        latencyWriter.close();
        componentWriter.close();
        mergeWriter.close();
        System.out.println(file.getName() + " average components per merge: " + (double) mergeComponents / merges);
    }

    private double processLatency(String line, List<Latency> list) {
        try {
            String[] subs = line.split(", ");
            int latency = Integer.valueOf(subs[1]);
            double count = Double.valueOf(subs[2]);
            list.add(new Latency((double) latency / 1000, (int) count));
            return count;
        } catch (Exception e) {
            System.out.println("Skip " + line);
            return 0;
        }
    }

    private int parseMerges(String line) {
        String ident = "Schedule merge ";
        int begin = line.indexOf(ident) + ident.length();
        int end = line.indexOf(" ", begin);
        String[] part = line.substring(begin, end).split("\\+");
        try {
            return 1 + Integer.valueOf(part[1]);
        } catch (Exception e) {
            System.out.println("Fail to parse " + line);
            return 0;
        }

    }

    private void processComponents(String line, PrintWriter writer) {
        try {
            String ident = "time:";
            int index = line.indexOf(ident) + ident.length();
            int time = Integer.valueOf(line.substring(index, line.indexOf(',', index)));

            ident = "#components:";
            index = line.indexOf(ident) + ident.length();
            int count = Integer.valueOf(line.substring(index, line.length()));
            writer.println(String.format("%.1f\t%d", (double) time / 1000, count));
        } catch (Exception e) {
            System.out.println("Skip " + line);
        }
    }

    public static void main(String[] args) throws IOException {
        new FlowControlProcessor().run();
    }

}
