package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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

    public FileReadExperiment(String dir, int numFiles, int pageSizeKB, int fileSizeMB) throws IOException {
        this.bytes = new byte[pageSizeKB * 1024];
        this.numPages = fileSizeMB * 1024 / pageSizeKB;
        File dirFile = new File(dir);
        if (!dirFile.isDirectory()) {
            throw new IllegalArgumentException(dir + " is not directory");
        }
        files = generateFiles(numFiles, dirFile);
    }

    private List<File> generateFiles(int numFiles, File baseDir) throws IOException {
        List<File> files = new ArrayList<>(numFiles);
        for (int i = 0; i < numFiles; i++) {
            File file = new File(baseDir, "test-" + i);
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            for (int p = 0; p < numPages; p++) {
                Arrays.fill(bytes, (byte) p);
                out.write(bytes);
            }
            out.close();
            files.add(file);
            System.out.println("Generated file " + file);
        }
        return files;
    }

    public void run() throws IOException {
        sequentialRead();
        randomRead();
    }

    private List<FileChannel> readFiles() throws IOException {
        List<FileChannel> ins = new ArrayList<>();
        for (File file : files) {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            channel.position(0);
            ins.add(channel);
        }
        return ins;
    }

    private void sequentialRead() throws IOException {
        ByteBuffer dest = ByteBuffer.wrap(bytes);
        long begin = System.currentTimeMillis();
        List<FileChannel> ins = readFiles();
        for (int i = 0; i < files.size(); i++) {
            FileChannel channel = ins.get(i);
            for (int p = 0; p < numPages; p++) {
                dest.rewind();
                int bytesRead = channel.read(dest, (long) p * bytes.length);
                if (bytesRead != bytes.length) {
                    throw new IllegalStateException("Wrong bytes read " + bytesRead);
                }
            }
            System.out.println("Finished sequential read file " + i);
            channel.close();
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
        List<FileChannel> ins = readFiles();

        while (ins.size() > 0) {
            int i = rand.nextInt(ins.size());
            dest.rewind();
            int ret = ins.get(i).read(dest, (long) pages.get(i) * bytes.length);
            if (ret != bytes.length) {
                throw new IllegalStateException("Wrong bytes read " + ret);
            }
            pages.set(i, pages.get(i) + 1);
            if (pages.get(i) >= numPages) {
                ins.get(i).close();
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
        if (args.length != 4) {
            System.err.println("args: dir numFiles pageSize(kb) fileSize(mb)");
            return;
        }

        String dir = args[0];
        int numFiles = Integer.valueOf(args[1]);
        int pageSizeKB = Integer.valueOf(args[2]);
        int fileSizeMB = Integer.valueOf(args[3]);
        System.out.println("File Read Experiment ");
        System.out.println("Num Files " + numFiles);
        System.out.println("PageSize (KB) " + pageSizeKB);
        System.out.println("FileSize (MB) " + fileSizeMB);

        FileReadExperiment expr = new FileReadExperiment(dir, numFiles, pageSizeKB, fileSizeMB);
        expr.run();
    }
}
