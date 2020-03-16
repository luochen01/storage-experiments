package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import net.smacke.jaydio.DirectRandomAccessFile;

/**
 * Experiment performance with sequential and random I/Os
 *
 * @author luochen
 *
 */
public class FileReadExperiment {
    private final byte[] bytes;

    private final List<File> files;

    private final int numPages;
    private final boolean directIO;

    public FileReadExperiment(String dir, int numFiles, int pageSizeKB, int fileSizeMB, boolean createFiles,
            boolean directIO) throws IOException {
        this.bytes = new byte[pageSizeKB * 1024];
        this.numPages = fileSizeMB * 1024 / pageSizeKB;
        File dirFile = new File(dir);
        if (!dirFile.isDirectory()) {
            throw new IllegalArgumentException(dir + " is not directory");
        }
        files = generateFiles(numFiles, dirFile, createFiles);
        this.directIO = directIO;
    }

    private List<File> generateFiles(int numFiles, File baseDir, boolean createFiles) throws IOException {
        List<File> files = new ArrayList<>(numFiles);
        for (int i = 0; i < numFiles; i++) {
            File file = new File(baseDir, "test-" + i);
            if (createFiles) {
                BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
                for (int p = 0; p < numPages; p++) {
                    Arrays.fill(bytes, (byte) p);
                    out.write(bytes);
                }
                out.close();
                System.out.println("Generated file " + file);
            } else {
                System.out.println("Loaded file " + file);
            }
            files.add(file);

        }
        return files;
    }

    public void run() throws IOException {
        //sequentialRead();
        randomRead();
    }

    private List<Object> readFiles() throws IOException {
        List<Object> ins = new ArrayList<>();
        for (File file : files) {
            ins.add(directIO ? new DirectRandomAccessFile(file, "rw") : new RandomAccessFile(file, "rw"));
        }
        return ins;
    }

    private void sequentialRead() throws IOException {
        ByteBuffer dest = ByteBuffer.wrap(bytes);
        long begin = System.currentTimeMillis();
        List<Object> ins = readFiles();
        for (int i = 0; i < files.size(); i++) {
            for (int p = 0; p < numPages; p++) {
                dest.rewind();
                int bytesRead =
                        directIO ? ((DirectRandomAccessFile) ins.get(i)).read(dest.array(), 0, dest.array().length)
                                : ((RandomAccessFile) ins.get(i)).read(dest.array(), 0, dest.array().length);
                if (bytesRead != bytes.length) {
                    throw new IllegalStateException("Wrong bytes read " + bytesRead);
                }
            }
            System.out.println("Finished sequential read file " + i);
        }
        long end = System.currentTimeMillis();
        long time = (end - begin);
        System.out.println("Sequential read finishes in " + time + " ms");
        System.out.println("Sequential read throughput " + computeThroughputKB(time) + " KB/s");
    }

    private long computeThroughputKB(long timeInMs) {
        long totalSizeKB = (long) numPages * bytes.length * files.size() / 1024;
        return (long) (totalSizeKB / ((double) timeInMs / 1000));
    }

    private void randomRead() throws IOException {
        ByteBuffer dest = ByteBuffer.wrap(bytes);
        long begin = System.currentTimeMillis();
        Random rand = new Random(System.currentTimeMillis());
        List<Integer> pages = new ArrayList<>(files.size());
        files.forEach(f -> pages.add(0));
        List<Object> ins = readFiles();

        while (ins.size() > 0) {
            int i = rand.nextInt(ins.size());
            dest.rewind();
            if (directIO) {
                DirectRandomAccessFile file = (DirectRandomAccessFile) ins.get(i);
                file.seek((long) pages.get(i) * bytes.length);
                file.read(bytes, 0, bytes.length);
            } else {
                RandomAccessFile file = (RandomAccessFile) ins.get(i);
                file.seek((long) pages.get(i) * bytes.length);
                file.read(bytes, 0, bytes.length);
            }
            pages.set(i, pages.get(i) + 1);
            if (pages.get(i) >= numPages) {
                System.out.println("Finished " + files.get(i));
                ins.remove(i);
                pages.remove(i);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Random read finishes in " + (end - begin) + " ms");
        System.out.println("Random read throughput " + computeThroughputKB(end - begin) + " KB/s");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 6) {
            System.err.println("args: dir numFiles pageSize(kb) fileSize(mb) createFiles(boolean) directIO(boolean)");
            return;
        }

        String dir = args[0];
        int numFiles = Integer.valueOf(args[1]);
        int pageSizeKB = Integer.valueOf(args[2]);
        int fileSizeMB = Integer.valueOf(args[3]);
        boolean createFiles = Boolean.valueOf(args[4]);
        boolean directIO = Boolean.valueOf(args[5]);
        System.out.println("File Read Experiment ");
        System.out.println("Num Files " + numFiles);
        System.out.println("PageSize (KB) " + pageSizeKB);
        System.out.println("FileSize (MB) " + fileSizeMB);
        System.out.println("Direct IO " + directIO);

        FileReadExperiment expr = new FileReadExperiment(dir, numFiles, pageSizeKB, fileSizeMB, createFiles, directIO);
        expr.run();
    }
}
