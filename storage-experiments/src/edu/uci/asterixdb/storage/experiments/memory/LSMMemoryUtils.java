package edu.uci.asterixdb.storage.experiments.memory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.common.base.Preconditions;

public class LSMMemoryUtils {

    public static final String[] workloads = new String[] { "write", "write-heavy", "read", "scan" };

    public static final int[] memories = new int[] { 128, 256, 512, 1024, 2048, 4096, 8192 };

    public static final int skip = 600;
    public static final int tpccSkip = 1800;

    public static File getFile(String basePath, Object... patterns) {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.contains(".log") || name.contains(".txt")) {
                    return false;
                }
                for (Object pattern : patterns) {
                    if (!name.contains(pattern.toString())) {
                        return false;
                    }
                }
                return true;
            }
        });
        Preconditions.checkState(files.length == 1, "Incorrect result files for %s. %s", Arrays.toString(patterns),
                Arrays.toString(files));
        return files[0];
    }

    public static File getLogFile(String basePath, Object... patterns) {
        File dir = new File(basePath);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                for (Object pattern : patterns) {
                    if (!name.contains(pattern.toString())) {
                        return false;
                    }
                }
                return true;
            }
        });
        if (files.length != 1) {
            System.out.println(String.format("Incorrect result files for %s. %s", Arrays.toString(patterns),
                    Arrays.toString(files)));
            return null;
        } else {
            return files[0];
        }
    }

    public static double getLogValue(File file, String pattern) throws NumberFormatException, IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.contains(pattern)) {
                    return Double.valueOf(line.substring(pattern.length()));
                }
            }
            return 0;
        }
    }

    public static int doComputeThroughput(File file, int skip, Function<CSVRecord, Integer> processor)
            throws IOException {
        if (file == null) {
            return 0;
        }
        CSVParser parser = new CSVParser(new FileReader(file), CSVFormat.newFormat('\t').withFirstRecordAsHeader());
        int count = 0;
        int validConut = 0;
        long totalOps = 0;
        for (CSVRecord record : parser) {
            count++;
            if (count > skip) {
                try {
                    int ops = processor.apply(record);
                    if (ops >= 0) {
                        totalOps += ops;
                        validConut++;
                    }
                } catch (Exception e) {
                    System.out.println("Error line " + record.toString());
                    e.printStackTrace();
                }

            }
            if (count >= 3600) {
                break;
            }
        }
        if (count < 1800) {
            System.out.println("incorrect result " + file.getName());
        }
        parser.close();
        return (int) (totalOps / Math.max(validConut, 1));
    }

    public static int computeThroughput(File file) throws IOException {
        return doComputeThroughput(file, skip,
                record -> Integer.valueOf(record.get(1)) + Integer.valueOf(record.get(3)));
    }

    public static int computeTPCCThroughput(File file) throws IOException {
        return doComputeThroughput(file, tpccSkip, record -> Integer.valueOf(record.get(1).split(",")[0]));
    }

    public static int computeTPCCDiskWrite(File file) throws IOException {
        return doComputeThroughput(file, tpccSkip, record -> Integer.valueOf(record.get(9).split(",")[0]));
    }

    public static int computeTPCCDiskRead(File file) throws IOException {
        return doComputeThroughput(file, tpccSkip, record -> {
            String[] parts = record.get(9).split(",");
            return Integer.valueOf(parts[1]);
        });
    }

    public static String toString(Object[] strs, String delim) {
        StringBuilder sb = new StringBuilder();
        for (Object obj : strs) {
            sb.append(obj);
            sb.append(delim);
        }
        return sb.toString();
    }

    public static String toString(Object param, Object[] strs, String delim) {
        StringBuilder sb = new StringBuilder();
        sb.append(param);
        sb.append(delim);
        for (Object obj : strs) {
            sb.append(obj);
            sb.append(delim);
        }
        return sb.toString();
    }

    public static String toString(int[] strs, String delim) {
        StringBuilder sb = new StringBuilder();
        for (int obj : strs) {
            sb.append(obj);
            sb.append(delim);
        }
        return sb.toString();
    }

    public static String toHeader(String param, Object[] values, String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(param);
        sb.append("\t");
        for (Object value : values) {
            sb.append(prefix + "-" + value);
            sb.append("\t");
        }
        return sb.toString();
    }

    public static Date parseDate(String part) {
        String[] parts = part.split(":");
        int hour = Integer.valueOf(parts[0]);
        int min = Integer.valueOf(parts[1]);
        int sec = Integer.valueOf(parts[2].split("\\.")[0]);
        return new Date(2020, 2, 11, hour, min, sec);
    }

    public static double parseWriteMBs(String str) {
        if (str.endsWith("MB")) {
            return Double.valueOf(str.substring(0, str.length() - 2));
        } else if (str.endsWith("GB")) {
            return Double.valueOf(str.substring(0, str.length() - 2)) * 1024;
        } else {
            return 0;
        }
    }
}
