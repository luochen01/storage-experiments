package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ProcessingUnit {
    String name;
    double recordSpeed;
    double byteSpeed;
    double componentSpeed;

    public ProcessingUnit(String name, double recordSpeed, double byteSpeed, double componentSpeed) {
        super();
        this.name = name;
        this.recordSpeed = recordSpeed;
        this.byteSpeed = byteSpeed;
        this.componentSpeed = componentSpeed;
    }

}

public class FlowNetworkParser {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/flowcontrol";

    private final String unitPattern = "-storage/partition_0/twitter/ds_tweet/0/ds_tweet";

    private final FilenameFilter filter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".log");
        }
    };

    public void run() throws IOException {
        File dir = new File(basePath);
        File[] files = dir.listFiles(filter);
        for (File file : files) {
            process(file);
        }
    }

    private void process(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        List<List<ProcessingUnit>> results = new ArrayList<>();
        List<ProcessingUnit> units = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            if (line.equals("source")) {
                if (!units.isEmpty()) {
                    results.add(units);
                    units = new ArrayList<>();
                }
                continue;
            }
            ProcessingUnit unit = parseProcessingUnit(line);
            if (unit != null) {
                units.add(unit);
            }
        }

        reader.close();

        File recordFile = new File(file.getParentFile(), file.getName() + ".records");
        BufferedWriter writer = new BufferedWriter(new FileWriter(recordFile));
        boolean first = true;
        int i = 1;
        for (List<ProcessingUnit> list : results) {
            if (first) {
                writer.write("time\t");
                for (ProcessingUnit unit : list) {
                    writer.write(unit.name);
                    writer.write("\t");
                }
                writer.write(System.lineSeparator());
                first = false;
            }
            StringBuilder sb = new StringBuilder();
            sb.append((i++));
            sb.append("\t");
            for (ProcessingUnit unit : list) {
                sb.append(unit.recordSpeed);
                sb.append("\t");
            }
            writer.write(sb.toString());
            writer.write(System.lineSeparator());
        }
        writer.close();

        System.out.println("Generated " + recordFile);
    }

    private ProcessingUnit parseProcessingUnit(String line) {
        if (!line.contains(unitPattern)) {
            return null;
        }

        int nameIndex = line.indexOf("ds_tweet-") + "ds_tweet-".length();
        int nameEndIndex = line.indexOf('(', nameIndex);
        String name = line.substring(nameIndex, nameEndIndex);

        int recordIndex = line.indexOf(": ", nameEndIndex) + 2;
        int recordEndIndex = line.indexOf("records/s", recordIndex);
        double recordSpeed = Double.valueOf(line.substring(recordIndex, recordEndIndex));

        int byteIndex = line.indexOf(", ", recordIndex) + 2;
        int byteEndIndex = line.indexOf("bytes/s", recordIndex);
        double byteSpeed = Double.valueOf(line.substring(byteIndex, byteEndIndex));

        int componentIndex = line.indexOf(", ", byteEndIndex) + 2;
        int componentEndIndex = line.indexOf("components/s", componentIndex);
        double componentSpeed = Double.valueOf(line.substring(componentIndex, componentEndIndex));

        return new ProcessingUnit(name, recordSpeed, byteSpeed, componentSpeed);
    }

    public static void main(String[] args) throws Exception {
        new FlowNetworkParser().run();
    }

}
