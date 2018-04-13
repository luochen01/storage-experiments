package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class ConcurrentFileWriteTest {

    public static void main(String args[]) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage: pageSize(KB), smallFileSize(MB), bigFileSize(MB), path, threads");
            return;
        }

        long pageSize = Long.parseLong(args[0]) * 1024;
        long smallFileSize = Long.parseLong(args[1]) * 1024 * 1024;
        long bigFileSize = Long.parseLong(args[2]) * 1024 * 1024;
        String file = args[3];
        int numThreads = Integer.parseInt(args[4]);

        System.out.println("PageSize: " + pageSize);
        System.out.println("SmallFileSize: " + smallFileSize);
        System.out.println("BigFileSize: " + bigFileSize);
        System.out.println("numThreads: " + numThreads);

        WriterThread[] threads = new WriterThread[numThreads];

        for (int i = 1; i < numThreads; i++) {
            threads[i] = new WriterThread(pageSize, bigFileSize, new File(file + "-" + i));
            threads[i].start();
        }
        threads[0] = new WriterThread(pageSize, smallFileSize, new File(file + "-" + 0));
        threads[0].start();

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        System.exit(0);
    }

    private static class WriterThread extends Thread {
        private final long numPages;
        private final byte[] bytes;
        private final File file;

        public WriterThread(long pageSize, long totalSize, File file) {
            this.bytes = new byte[(int) pageSize];
            Arrays.fill(bytes, (byte) 5);
            this.file = file;
            this.numPages = totalSize / pageSize;
        }

        @Override
        public void run() {
            try {
                long begin = System.currentTimeMillis();
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                FileChannel channel = raf.getChannel();
                channel.position(0);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                for (long i = 0; i < numPages; i++) {
                    buffer.clear();
                    channel.write(buffer);
                }
                channel.force(true);
                raf.close();
                long end = System.currentTimeMillis();
                System.out.println("Finished " + numPages + " pages in " + (end - begin) + " ms");
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

}
