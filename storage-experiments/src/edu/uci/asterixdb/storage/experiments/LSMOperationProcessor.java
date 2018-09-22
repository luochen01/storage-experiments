package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

class OperationStat {
    public double time;
    public double speed;
    public int dirty;

    public OperationStat(double time, double speed, int dirty) {
        this.time = time;
        this.speed = speed;
        this.dirty = dirty;
    }

}

class ProcessingUnitStat {
    public final SortedMap<Integer, List<OperationStat>> initializeMap = new TreeMap<>();
    public final SortedMap<Integer, List<OperationStat>> processingMap = new TreeMap<>();
    public final SortedMap<Integer, List<OperationStat>> finalizeMap = new TreeMap<>();

    private void addOp(SortedMap<Integer, List<OperationStat>> map, int op, OperationStat stat) {
        List<OperationStat> list = map.computeIfAbsent(op, k -> new ArrayList<>());
        list.add(stat);
    }

    public void addInitializeOp(int op, OperationStat stat) {
        addOp(initializeMap, op, stat);
    }

    public void addProcessingOp(int op, OperationStat stat) {
        addOp(processingMap, op, stat);
    }

    public void addFinalizeOp(int op, OperationStat stat) {
        addOp(finalizeMap, op, stat);
    }
}

public class LSMOperationProcessor {

    private final String basePath = "/Users/luochen/Desktop/log/tier_limit_history_50.log";

    private final String outputPath = "/Users/luochen/Desktop/log/tier_";

    private final double validPages = 32;

    public static void main(String[] args) throws IOException {
        new LSMOperationProcessor().run();;
    }

    public void run() throws IOException {
        parse(new File(basePath));
    }

    private void parse(File file) throws IOException {
        SortedMap<String, ProcessingUnitStat> resultMap = new TreeMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (!line.contains("phase") || !line.contains("AbstractLSMProcessingUnit")) {
                continue;
            }
            String unit = getUnitName(line);
            ProcessingUnitStat stat = resultMap.computeIfAbsent(unit, k -> new ProcessingUnitStat());

            String phase = getPhase(line);
            double time = getTime(line);
            double input = getRecords(line);
            int dirty = getDirty(line);
            OperationStat opStat = new OperationStat(time, input / time * 1000, dirty);
            int operations = getOperations(line);
            switch (phase) {
                case "Initialize":
                    stat.addInitializeOp(operations, opStat);
                    break;
                case "Processing":
                    stat.addProcessingOp(operations, opStat);
                    break;
                case "Finalize":
                    stat.addFinalizeOp(operations, opStat);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown phase " + phase);
            }
        }

        reader.close();

        for (String unit : resultMap.keySet()) {
            ProcessingUnitStat stat = resultMap.get(unit);
            File outFile = new File(outputPath + unit);
            PrintWriter writer = new PrintWriter(new FileWriter(outFile));
            boolean hasRemaining = true;
            int ops = stat.processingMap.size() + 1;
            // output header
            for (int i = 1; i <= ops; i++) {
                writer.print("initialize-" + i);
                writer.print("\t");
                writer.print("initialize-" + i);
                writer.print("\t");
            }
            for (int i = 1; i <= ops; i++) {
                writer.print("process-" + i);
                writer.print("\t");
                writer.print("process-" + i);
                writer.print("\t");
            }
            for (int i = 1; i <= ops; i++) {
                writer.print("finalize-" + i);
                writer.print("\t");
                writer.print("finalize-" + i);
                writer.print("\t");
            }
            writer.println();
            int index = 0;
            while (hasRemaining) {
                hasRemaining = false;
                hasRemaining = printStat(stat.initializeMap, writer, ops, index, false) || hasRemaining;
                hasRemaining = printStat(stat.processingMap, writer, ops, index, true) || hasRemaining;
                hasRemaining = printStat(stat.finalizeMap, writer, ops, index, false) || hasRemaining;
                writer.println();
                index++;
            }

            writer.close();
            System.out.println("Produced " + outFile);
        }
    }

    private boolean printStat(Map<Integer, List<OperationStat>> map, PrintWriter writer, int ops, int index,
            boolean printSpeed) {
        boolean hasRemaining = false;
        for (int i = 1; i <= ops; i++) {
            List<OperationStat> list = map.get(i);
            if (list != null && list.size() > index) {
                OperationStat stat = list.get(index);
                if (printSpeed) {
                    writer.print(stat.speed);
                    writer.print("\t");
                } else {
                    writer.print(stat.time);
                    writer.print("\t");
                }
                writer.print(stat.dirty);
                writer.print("\t");
                hasRemaining = true;
            } else {
                writer.print("\t");
                writer.print("\t");
            }
        }
        return hasRemaining;
    }

    private String getUnitName(String line) {
        String del = " - ";
        String endDel = ": ";
        int index = line.indexOf(del);
        int endIndex = line.indexOf(endDel, index);
        String name = line.substring(index + del.length(), endIndex);
        String[] parts = name.split("/");
        return parts[parts.length - 1];
    }

    private String getPhase(String line) {
        String del = "phase ";
        String endDel = ",";
        int index = line.indexOf(del);
        int endIndex = line.indexOf(endDel, index);
        return line.substring(index + del.length(), endIndex);
    }

    private double getPages(String line) {
        String del = "pages ";
        String endDel = ",";
        int index = line.indexOf(del);
        int endIndex = line.indexOf(endDel, index);
        return Double.valueOf(line.substring(index + del.length(), endIndex));
    }

    private double getTime(String line) {
        String del = "time ";
        String endDel = " ms";
        int index = line.indexOf(del);
        int endIndex = line.indexOf(endDel, index);
        return Double.valueOf(line.substring(index + del.length(), endIndex));
    }

    private int getOperations(String line) {
        String del = "operations ";
        int index = line.indexOf(del);
        int endIndex = line.indexOf(",", index);
        return Integer.valueOf(line.substring(index + del.length(), endIndex));
    }

    private double getRecords(String line) {
        String del = "input records ";
        int index = line.indexOf(del);
        int endIndex = line.indexOf(",", index);
        return Double.valueOf(line.substring(index + del.length(), endIndex));
    }

    private int getDirty(String line) {
        String del = "dirty ";
        int index = line.indexOf(del);
        int endIndex = line.length();
        return Integer.valueOf(line.substring(index + del.length(), endIndex));
    }

}
