package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConcurrentFileWriteTest {

    private static long pageSize;
    private static long baseFileSize;
    private static String file;
    private static int numThreads;
    private static int forceFrequency;
    private static final AtomicLong totalPages = new AtomicLong();

    public static void main(String args[]) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: pageSize(KB), baseFileSize(MB), path, threads, runs, [forceFrequency (pages)]");
            return;
        }

        pageSize = Long.parseLong(args[0]) * 1024;
        baseFileSize = Long.parseLong(args[1]) * 1024 * 1024;
        file = args[2];
        numThreads = Integer.parseInt(args[3]);
        int runs = Integer.parseInt(args[4]);
        forceFrequency = 0;
        if (args.length > 5) {
            forceFrequency = Integer.parseInt(args[5]);
        }

        System.out.println("PageSize: " + pageSize);
        System.out.println("BaseFileSize: " + baseFileSize);
        System.out.println("numThreads: " + numThreads);
        System.out.println("Runs: " + runs);

        long begin = System.currentTimeMillis();
        WriterThread[] threads = new WriterThread[numThreads];
        long fileSize = baseFileSize;
        int run = runs;
        for (int i = 0; i < numThreads; i++) {
            System.out.println(run + " runs for writer-" + i);
            threads[i] = new WriterThread("writer-" + i, pageSize, fileSize, run);
            threads[i].start();

            run = Math.max(1, run / 2);
            fileSize = fileSize * 2;
        }

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println("Writing " + totalPages.get() * pageSize + " bytes takes " + (end - begin) + " ms");
        System.exit(0);
    }

    private static class WriterThread extends Thread {
        private final long numPages;
        private final byte[] bytes;
        private final int runs;
        private final String threadname;
        private final Random random;
        private final Logger LOGGER;

        public WriterThread(String threadname, long pageSize, long totalSize, int runs) {
            this.threadname = threadname;
            this.bytes = new byte[(int) pageSize];
            Arrays.fill(bytes, (byte) 5);
            this.numPages = totalSize / pageSize;
            this.runs = runs;
            this.random = new Random(threadname.hashCode());
            this.LOGGER = LogManager.getLogger(threadname);
        }

        @Override
        public void run() {
            for (int run = 0; run < runs; run++) {
                try {
                    File targetFile = new File(file + "-" + threadname + "-" + run);
                    long begin = System.currentTimeMillis();
                    targetFile.delete();
                    targetFile.createNewFile();
                    RandomAccessFile raf = new RandomAccessFile(targetFile, "rw");
                    FileChannel channel = raf.getChannel();
                    channel.position(0);
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    for (long i = 0; i < numPages; i++) {
                        random.nextBytes(bytes);
                        buffer.clear();
                        channel.write(buffer);
                        totalPages.incrementAndGet();
                    }
                    channel.force(false);
                    raf.close();
                    long end = System.currentTimeMillis();
                    LOGGER.error("Finished {} pages in {} ms", numPages, (end - begin));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
