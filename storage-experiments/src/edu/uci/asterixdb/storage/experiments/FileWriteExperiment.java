package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

public class FileWriteExperiment {
    private static volatile long totalBytesWriten = 0;

    public static void main(String args[]) throws Exception {
        testFileStream(args);
    }

    private static void testFileStream(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: path");
            return;
        }

        int pageSize = 32 * 1024;

        byte[] buffer = new byte[pageSize];

        File dir = new File(args[0]);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                file.delete();
            }
        } else {
            dir.mkdirs();
        }

        int fileId = 0;
        while (true) {
            fileId++;
            // we write 1GB data
            long pages = 1 * 1024 * 1024 * 1024 / pageSize;
            File file = new File(args[0], fileId + ".log");
            FileOutputStream out = new FileOutputStream(file);
            long begin = System.currentTimeMillis();
            for (int i = 0; i < pages; i++) {
                Arrays.fill(buffer, (byte) i);
                out.write(buffer);
                if (i != 0 && i % 320 == 0) {
                    long end = System.currentTimeMillis();
                    Thread.sleep(100 - (end - begin));
                    begin = end;
                }
            }
            out.close();
            System.out.println("deleting " + file);
            file.delete();
        }

    }

}
