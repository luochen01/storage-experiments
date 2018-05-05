package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Experiment performance with sequential and random I/Os
 *
 * @author luochen
 *
 */
public class FileSkipReadExperiment {
    private final byte[] bytes;

    private final File file;

    private final int numPages;
    private final double selectivity;
    private final Random rand = new Random();

    public FileSkipReadExperiment(String path, int pageSizeKB, int fileSizeMB, double selectivity) throws IOException {
        this.bytes = new byte[pageSizeKB * 1024];
        this.numPages = fileSizeMB * 1024 / pageSizeKB;
        this.selectivity = selectivity;
        file = generateFile(path, fileSizeMB);
    }

    private File generateFile(String path, int fileSizeMB) throws IOException {
        long fileSize = fileSizeMB * 1024 * 1024;
        File file = new File(path);
        if (file.exists()) {
            if (file.length() >= fileSize) {
                System.out.println("file already exists " + file.length());
                return file;
            } else {
                System.out.println(
                        "delete old file because file size too small " + file.length() + " expected size " + fileSize);
                file.delete();
            }
        }
        FileOutputStream out = new FileOutputStream(file);
        for (int p = 0; p < numPages; p++) {
            rand.nextBytes(bytes);
            out.write(bytes);
        }
        out.getChannel().force(false);
        out.close();
        return file;
    }

    public void run() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();
        long dummy = 0;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        List<Integer> pageList = new ArrayList<>();
        for (int i = 0; i < numPages; i++) {
            pageList.add(i);
        }
        Collections.shuffle(pageList);

        int numReadPages = (int) (numPages * selectivity);
        List<Integer> readPages = new ArrayList<>();
        for (int i = 0; i < numReadPages; i++) {
            readPages.add(pageList.get(i));
        }
        Collections.sort(readPages);
        System.out.println("Start reading " + readPages.size() + " pages");
        long begin = System.currentTimeMillis();
        long totalPages = 0;
        for (int page : readPages) {
            channel.position(page * bytes.length);
            buffer.rewind();
            if (channel.read(buffer) != bytes.length) {
                throw new IllegalStateException("Illegal bytes read");
            }
            for (byte b : bytes) {
                dummy += b;
            }
            totalPages++;
        }
        raf.close();
        long end = System.currentTimeMillis();

        System.out.println("Read " + totalPages + " pages in " + (end - begin) + " ms");
        System.out.println(dummy);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("args: file pageSize(kb) fileSize(mb) selectivity");
            return;
        }

        String path = args[0];
        int pageSizeKB = Integer.valueOf(args[1]);
        int fileSizeMB = Integer.valueOf(args[2]);
        double selectivity = Double.valueOf(args[3]);
        System.out.println("File Read Experiment ");
        System.out.println("PageSize (KB) " + pageSizeKB);
        System.out.println("FileSize (MB) " + fileSizeMB);
        System.out.println("Selectivity " + selectivity);

        FileSkipReadExperiment expr = new FileSkipReadExperiment(path, pageSizeKB, fileSizeMB, selectivity);
        expr.run();
    }
}
