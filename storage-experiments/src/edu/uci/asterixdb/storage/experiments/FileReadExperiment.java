package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Experiment performance with sequential and random I/Os
 *
 * @author luochen
 *
 */
public class FileReadExperiment {

    private final int bufferSize;

    private final char[] buffer;

    private final List<FileEntry> files = new ArrayList<>();

    private class FileEntry {
        File file;
        FileReader reader;

        public FileEntry(File file) throws IOException {
            this.file = file;
        }

        public void open() throws IOException {
            reader = new FileReader(file);
        }

        public void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }
    }

    public FileReadExperiment(String dir, int numFiles, int bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        this.buffer = new char[bufferSize];
        File dirFile = new File(dir);
        if (!dirFile.isDirectory()) {
            throw new IllegalArgumentException(dir + " is not directory");
        }
        File[] files = dirFile.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                // only consider large btrees
                return name.endsWith("_b");
            }
        });
        for (int i = 0; i < Math.min(numFiles, files.length); i++) {
            this.files.add(new FileEntry(files[i]));
            System.out.println("Collect " + files[i] + " with size " + files[i].length() / (1024 * 1024) + "MB");
        }
        System.out.println("Collect " + this.files.size() + " for experiments");
    }

    public void run() throws IOException {
        sequentialRead();
        randomRead();
    }

    private void sequentialRead() throws IOException {
        long begin = System.currentTimeMillis();
        for (FileEntry entry : files) {
            entry.open();

            while (entry.reader.read(buffer, 0, bufferSize) != -1) {

            }

            System.out.println("Finished " + entry.file);
            entry.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("Sequential read finishes in " + (end - begin) + " ms");
    }

    private void randomRead() throws IOException {
        long begin = System.currentTimeMillis();
        Random rand = new Random(System.currentTimeMillis());
        for (FileEntry e : files) {
            e.open();
        }

        while (files.size() > 0) {
            int i = rand.nextInt(files.size());
            if (files.get(i).reader.read(buffer, 0, bufferSize) == -1) {
                files.get(i).close();
                System.out.println("Finished " + files.get(i).file);
                files.remove(i);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Random read finishes in " + (end - begin) + " ms");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("args: dir numFiles bufferSize(kb)");
            return;
        }

        String dir = args[0];
        int numFiles = Integer.valueOf(args[1]);
        int bufferSize = Integer.valueOf(args[2]) * 1024;

        System.out.println("File Read Experiment ");
        System.out.println("Num Files " + numFiles);
        System.out.println("Buffer Size " + bufferSize);

        FileReadExperiment expr = new FileReadExperiment(dir, numFiles, bufferSize);
        expr.run();
    }
}
