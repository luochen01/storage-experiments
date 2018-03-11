package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Comparator;
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

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/inplace/ingestion";

    private final int recordIndex = 1;

    private final int totalRecordIndex = 3;

    private CSVFile parseCSV(File file) throws Exception {
        List<CSVObject> result = new ArrayList<>();
        Reader reader = new FileReader(file);
        CSVFormat format = CSVFormat.newFormat(',').withFirstRecordAsHeader();
        Iterable<CSVRecord> records = format.parse(reader);
        for (CSVRecord record : records) {
            long recordNum = Long.valueOf(record.get(recordIndex));
            long totalRecordNum = Long.valueOf(record.get(totalRecordIndex));
            result.add(new CSVObject(recordNum, totalRecordNum));
        }
        return new CSVFile(file.getName(), result);
    }

    private void writeCSV(List<CSVFile> files) throws FileNotFoundException {
        files.sort(new Comparator<CSVFile>() {
            //            public int compare(CSVFile o1, CSVFile o2) {
            //                try {
            //                    String name1 = o1.name.substring(0, o1.name.indexOf('.'));
            //                    String name2 = o2.name.substring(0, o2.name.indexOf('.'));
            //                    String[] splits1 = name1.split("_");
            //                    String[] splits2 = name2.split("_");
            //
            //                    String num1 = numericalPrefix(splits1[splits1.length - 1]);
            //                    String num2 = numericalPrefix(splits2[splits2.length - 1]);
            //                    return Integer.compare(Integer.valueOf(num1), Integer.valueOf(num2));
            //                } catch (Exception e) {
            //                    return 0;
            //                }
            //            }

            @Override
            public int compare(CSVFile o1, CSVFile o2) {
                return o1.name.compareTo(o2.name);
            }

            private String numericalPrefix(String value) {
                for (int i = 0; i < value.length(); i++) {
                    if (!Character.isDigit(value.charAt(i))) {
                        return value.substring(0, i);
                    }
                }
                return value;
            }
        });

        File recordFile = new File(basePath, "record.csv");
        File totalRecordFile = new File(basePath, "total.csv");

        PrintWriter recordWriter = new PrintWriter(recordFile);
        PrintWriter totalWriter = new PrintWriter(totalRecordFile);

        // print header
        StringBuilder header = new StringBuilder();
        header.append("time,");
        for (int i = 0; i < files.size(); i++) {
            header.append(files.get(i).name);
            if (i < files.size() - 1) {
                header.append(",");
            }
        }

        recordWriter.println(header);
        totalWriter.println(header);
        int row = 0;
        while (true) {
            int stopCount = 0;
            StringBuilder recordBuilder = new StringBuilder();
            StringBuilder totalBuilder = new StringBuilder();
            recordBuilder.append(row + 1);
            recordBuilder.append(",");
            totalBuilder.append(row + 1);
            totalBuilder.append(",");
            for (int i = 0; i < files.size(); i++) {
                CSVFile file = files.get(i);
                if (row < file.objects.size()) {
                    CSVObject obj = file.objects.get(row);
                    recordBuilder.append(obj.record);
                    totalBuilder.append(obj.totalRecord);
                } else {
                    stopCount++;
                }

                if (i < files.size() - 1) {
                    recordBuilder.append(',');
                    totalBuilder.append(',');
                }
            }
            if (stopCount == files.size()) {
                break;
            }
            recordWriter.println(recordBuilder);
            totalWriter.println(totalBuilder);
            row++;
        }

        recordWriter.close();
        totalWriter.close();

    }

    public void run() throws Exception {
        File baseDir = new File(basePath);
        File[] files = baseDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return !name.contains("DS_Store") && name.contains("ingest") && name.contains(".log");
            }
        });
        List<CSVFile> results = new ArrayList<>();
        for (File file : files) {
            try {
                results.add(parseCSV(file));
                System.out.println("Parsed " + file.getName());
            } catch (Exception e) {
                System.err.println("Invalid CSV file " + file);
                e.printStackTrace();
            }
        }
        writeCSV(results);
    }

    public static void main(String[] args) throws Exception {
        new IngestCSVProcessor().run();
    }

}
