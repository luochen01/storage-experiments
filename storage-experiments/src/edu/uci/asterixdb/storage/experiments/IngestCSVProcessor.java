package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

class CSVObject {
    long record;
    long totalRecord;

    public CSVObject(long record, long totalRecord) {
        super();
        this.record = record;
        this.totalRecord = totalRecord;
    }
}

class CSVFile {
    String name;
    List<CSVObject> objects;

    public CSVFile(String name, List<CSVObject> objects) {
        super();
        this.name = name;
        this.objects = objects;
    }

}

public class IngestCSVProcessor {

    private static final String basePath = "/Users/luochen/Desktop/twitter.log";

    private final int recordIndex = 1;

    private final int totalRecordIndex = 3;

    private final int window = 300;

    private void parseCSV(File file) throws Exception {
        Reader reader = new FileReader(file);
        CSVFormat format = CSVFormat.newFormat(',').withFirstRecordAsHeader();
        int sum = 0;
        Iterable<CSVRecord> records = format.parse(reader);
        int count = 0;
        int total = 0;
        for (CSVRecord record : records) {
            sum += Integer.valueOf(record.get(recordIndex));
            count++;
            total++;
            if (count == window) {
                System.out.println(total + "\t" + sum / count);
                sum = 0;
                count = 0;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new IngestCSVProcessor().parseCSV(new File(basePath));
    }

}
