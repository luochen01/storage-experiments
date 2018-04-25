package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class FileLatencyExperiment {

    public static void main(String args[]) throws Exception {
        File dir = new File(args[0]);
        dir.mkdirs();
        int numThreads = Integer.parseInt(args[1]);

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new FileWriteThread(new File(dir, i + ".write")));
            threads[i].start();
        }

        int i = 0;
        while (true) {
            long begin = System.currentTimeMillis();
            File newFile = new File(dir, i + ".create");
            long end = System.currentTimeMillis();
            System.out.println("Create " + newFile + " takes " + (end - begin) + " ms");
            newFile.createNewFile();
            Thread.sleep(500);
            i++;
        }

    }

    private static class FileWriteThread implements Runnable {

        private final File file;

        public FileWriteThread(File file) {
            this.file = file;
        }

        @Override
        public void run() {
            if (file.exists()) {
                file.delete();
            }
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                FileChannel channel = raf.getChannel();
                byte[] bytes = new byte[128 * 1024];
                Arrays.fill(bytes, (byte) 119);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (true) {
                    buffer.rewind();
                    channel.write(buffer);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

}
