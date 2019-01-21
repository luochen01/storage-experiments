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

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/flowcontrol/level";

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
        BufferedReader reader = new BufferedReader(new FileReader(file));
        PrintWriter latencyWriter = new PrintWriter(new FileWriter(new File(file.getAbsolutePath() + ".latency.txt")));
        PrintWriter mergeWriter = new PrintWriter(new FileWriter(new File(file.getAbsolutePath() + ".merge.txt")));

        List<Latency> latencies = new ArrayList<>();
        latencyWriter.println("latency\tcount\tpercentage");
        PrintWriter componentWriter =
                new PrintWriter(new FileWriter(new File(file.getAbsolutePath() + ".component.txt")));
        componentWriter.println("time\tcount");
        mergeWriter.println("#components");
        String line = null;
        double totalCount = 0;
        long mergeComponents = 0;
        long merges = 0;
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
            }
        }

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
