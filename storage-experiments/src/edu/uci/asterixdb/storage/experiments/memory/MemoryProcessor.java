package edu.uci.asterixdb.storage.experiments.memory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

class Result implements Comparable<Result> {
    final String param;
    final long smallMemoryUsage;
    final long largeMemoryUsage;
    final long maxMemoryusage;
    final long writes;

    public Result(String param, long writes, long smallMemoryUsage, long largeMemoryUsage, long maxMemoryUsage) {
        super();
        this.param = param;
        this.writes = writes;
        this.smallMemoryUsage = smallMemoryUsage;
        this.largeMemoryUsage = largeMemoryUsage;
        this.maxMemoryusage = maxMemoryUsage;
    }

    @Override
    public int compareTo(Result o) {
        return Double.compare(Double.valueOf(param), Double.valueOf(o.param));
    }

}

public class MemoryProcessor {

    private final long skipWrites = 600;

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/memory/write";
    private final String outputPath = "/Users/luochen/Documents/Research/experiments/results/memory";

    private static final int WRITE_INDEX = 1;
    private static final int SMALL_INDEX = 5;
    private static final int LARGE_INDEX = 6;
    private static final int MAX_MEMORY_INDEX = 7;

    public void run(String method, String expr) throws IOException {
        File dir = new File(basePath);

        List<Result> results = new ArrayList<>();

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(expr) && name.contains(method) && !name.contains(".log");
            }
        });
        for (File file : files) {
            results.add(parseResult(file));
        }
        Collections.sort(results);
        // output
        PrintWriter writer = new PrintWriter(new FileWriter(new File(outputPath, method + "-" + expr)));

        writer.println(expr + "\t" + "writes\tsmall memory\tlarge memory\tmax memory");
        for (Result result : results) {
            writer.println(result.param + "\t" + result.writes + "\t" + result.smallMemoryUsage + "\t"
                    + result.largeMemoryUsage + "\t" + result.maxMemoryusage);
        }

        writer.close();
    }

    private Result parseResult(File file) throws IOException {
        System.out.println("Processing " + file.getName());

        CSVParser parser = CSVParser.parse(new FileReader(file), CSVFormat.TDF);
        Iterator<CSVRecord> iterator = parser.iterator();
        iterator.next();
        for (int i = 0; i < skipWrites && iterator.hasNext(); i++) {
            iterator.next();
        }
        long writes = 0;
        long smallMemoryUsage = 0;
        long largeMemoryUsage = 0;
        long count = 0;
        long maxMemoryUsage = 0;
        writes = 0;
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            count++;
            writes += Long.valueOf(record.get(WRITE_INDEX));
            smallMemoryUsage += Long.valueOf(record.get(SMALL_INDEX));
            largeMemoryUsage += Long.valueOf(record.get(LARGE_INDEX));
            maxMemoryUsage = Math.max(maxMemoryUsage, Long.valueOf(record.get(MAX_MEMORY_INDEX)));
        }

        count = Math.max(count, 1);

        String[] parts = file.getName().split("-");
        return new Result(parts[parts.length - 1], writes / count, smallMemoryUsage / count, largeMemoryUsage / count,
                maxMemoryUsage);
    }

    public static void main(String[] args) throws IOException {
        MemoryProcessor processor = new MemoryProcessor();

        processor.run("btree", "page");
        processor.run("btree", "value-size");

        String[] methods = new String[] { "hybrid", "leveling" };
        String[] exprs = new String[] { "large-page", "segment", "mutable", "size-ratio", "value-size" };

        for (String method : methods) {
            for (String expr : exprs) {
                processor.run(method, expr);
            }
        }

        processor.run("hybrid", "split-factor");
    }

}
