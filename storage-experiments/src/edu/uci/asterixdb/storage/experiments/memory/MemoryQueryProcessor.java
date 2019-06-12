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

class QueryResult implements Comparable<QueryResult> {
    final String param;
    final long writes;
    final long queries;
    final long maxMemory;

    public QueryResult(String param, long writes, long queries, long maxMemory) {
        super();
        this.param = param;
        this.writes = writes;
        this.queries = queries;
        this.maxMemory = maxMemory;
    }

    @Override
    public int compareTo(QueryResult o) {
        try {
            return Double.compare(Double.valueOf(param), Double.valueOf(o.param));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return 0;
        }
    }

}

public class MemoryQueryProcessor {

    private final long skipWrites = 600;

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/memory/query";
    private final String outputPath = "/Users/luochen/Documents/Research/experiments/results/memory";

    private static final int WRITE_INDEX = 1;
    private static final int QUERY_INDEX = 3;
    private static final int MEMORY_INDEX = 7;

    public void run(String method, String expr) throws IOException {
        File dir = new File(basePath);

        List<QueryResult> results = new ArrayList<>();

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("-" + expr + "-") && name.contains(method) && !name.contains(".log");
            }
        });
        for (File file : files) {
            results.add(parseResult(file));
        }
        Collections.sort(results);
        // output
        PrintWriter writer = new PrintWriter(new FileWriter(new File(outputPath, method + "-" + expr)));

        writer.println(expr + "\t" + "writes\tqueries\tmemory usage");
        for (QueryResult result : results) {
            writer.println(result.param + "\t" + result.writes + "\t" + result.queries + "\t" + result.maxMemory);
        }

        writer.close();
    }

    private QueryResult parseResult(File file) throws IOException {
        System.out.println("Processing " + file.getName());

        CSVParser parser = CSVParser.parse(new FileReader(file), CSVFormat.TDF);
        Iterator<CSVRecord> iterator = parser.iterator();
        iterator.next();
        for (int i = 0; i < skipWrites && iterator.hasNext(); i++) {
            iterator.next();
        }
        long writes = 0;
        long queries = 0;
        long maxMemory = 0;
        long count = 0;
        writes = 0;
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            count++;
            writes += Long.valueOf(record.get(WRITE_INDEX));
            queries += Long.valueOf(record.get(QUERY_INDEX));
            maxMemory = Long.max(maxMemory, Long.valueOf(record.get(MEMORY_INDEX)));
        }

        count = Math.max(count, 1);

        String[] parts = file.getName().split("-");
        return new QueryResult(parts[parts.length - 1], writes / count, queries / count, maxMemory);
    }

    public static void main(String[] args) throws IOException {
        MemoryQueryProcessor processor = new MemoryQueryProcessor();

        processor.run("btree", "lookup");
        processor.run("hybrid", "lookup");
        processor.run("hybrid", "lookup-bf");
        processor.run("leveling", "lookup");
        processor.run("leveling", "lookup-bf");

        String[] methods = new String[] { "btree", "hybrid", "leveling" };
        String[] exprs = new String[] { "", "scan", "value-size", "segment", "segment+bf" };

        for (String method : methods) {
            for (String expr : exprs) {
                processor.run(method, expr);
            }
        }

    }

}
