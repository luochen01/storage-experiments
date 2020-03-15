package edu.uci.asterixdb.storage.experiments.memory;

import static edu.uci.asterixdb.storage.experiments.memory.LSMMemoryUtils.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class SplitMemoryProcessor {

    private final String basePath =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/multi-lsm/var-skew-new";

    //private final String basePath = "/tmp";

    private final int skip = 600;

    private final String pattern;

    private final String[] policies = new String[] { "maxmemory", "minlsn", "adaptive" };

    private static final String[] portions2 = new String[] { "0.5,0.5", "0.6,0.4", "0.7,0.3", "0.8,0.2", "0.9,0.1" };
    private static final String[] portions10 =
            new String[] { "0.4,0.4,0.025,0.025,0.025,0.025,0.025,0.025,0.025,0.025" };

    private final String workload;
    private final String[] portions;
    private final String[] portionNames = new String[] { "50-50", "60-40", "70-30", "80-20", "90-10" };

    public SplitMemoryProcessor(String workload, String pattern, String[] portions) {
        this.workload = workload;
        this.pattern = pattern;
        this.portions = portions;

    }

    public void run() throws IOException {
        for (String policy : policies) {
            for (String portion : portions) {
                File file = getFile(basePath, pattern, workload, 1024, policy, portion);
                process(file);
            }
        }
    }

    public void runMulti() throws IOException {
        for (int mem : memories) {
            File file = getFile(basePath, pattern, workload, mem);
            process(file);
        }
    }

    private void process(File file) throws IOException {
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.newFormat('\t').withFirstRecordAsHeader());
        PrintWriter output = new PrintWriter(new File(file.getParentFile(), file.getName() + ".txt"));
        for (CSVRecord record : parser) {
            String memory = record.get(9);
            output.println(memory.replace(",", "\t"));
        }
        parser.close();
        output.close();
    }

    public static void main(String[] args) throws IOException {
        //new SplitMemoryProcessor("write-10", "btree", portions10).run();
        new SplitMemoryProcessor("write-10", "partition", portions10).run();
    }

}
