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
        testFileChannel(args);
        //testFileStream(args);
    }

    private static void testFileStream(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: File Size (MB), Page Size (KB), ReportFrequency (ms), file1, file2, ...");
            return;
        }

        long fileSize = Long.valueOf(args[0]) * 1024 * 1024;
        int pageSize = Integer.valueOf(args[1]) * 1024;
        int period = Integer.valueOf(args[2]);

        byte[] buffer = new byte[pageSize];

        File[] files = new File[args.length - 3];
        FileOutputStream[] streams = new FileOutputStream[files.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(args[i + 3]);
            FileUtils.deleteQuietly(files[i]);
            streams[i] = new FileOutputStream(files[i]);
        }

        int numPages = (int) (fileSize / pageSize);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            int counter = 0;
            long lastBytesWritten = 0;

            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                long totalBytesWrittenLocal = totalBytesWriten;
                sb.append(counter);
                sb.append(',');
                sb.append(totalBytesWrittenLocal - lastBytesWritten);
                sb.append(',');
                sb.append(totalBytesWrittenLocal);
                System.out.println(sb.toString());
                lastBytesWritten = totalBytesWrittenLocal;
                counter += period;
            }
        }, 0, period);

        long begin = System.nanoTime();
        for (int i = 0; i < numPages; i++) {
            Arrays.fill(buffer, (byte) i);
            for (FileOutputStream stream : streams) {
                stream.write(buffer);
                totalBytesWriten += buffer.length;
            }
        }
        for (int i = 0; i < files.length; i++) {
            streams[i].flush();
            streams[i].close();
        }

        long end = System.nanoTime();
        System.out.println("Finish write in " + (end - begin) / 1000000 + " ms");
        timer.cancel();
        System.exit(0);
    }

    private static void testFileChannel(String[] args) throws Exception {
        long fileSize = Long.valueOf(args[0]) * 1024 * 1024;
        int pageSize = Integer.valueOf(args[1]) * 1024;
        int period = Integer.valueOf(args[2]);

        byte[] buffer = new byte[pageSize];

        File[] files = new File[args.length - 3];
        RandomAccessFile[] accessFiles = new RandomAccessFile[args.length - 3];
        FileChannel[] fileChannels = new FileChannel[args.length - 3];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(args[i + 3]);
            FileUtils.deleteQuietly(files[i]);
            accessFiles[i] = new RandomAccessFile(files[i], "rw");
            fileChannels[i] = accessFiles[i].getChannel();
            fileChannels[i].position(0);
        }

        int numPages = (int) (fileSize / pageSize);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            int counter = 0;
            long lastBytesWritten = 0;

            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                long totalBytesWrittenLocal = totalBytesWriten;
                sb.append(counter);
                sb.append(',');
                sb.append(totalBytesWrittenLocal - lastBytesWritten);
                sb.append(',');
                sb.append(totalBytesWrittenLocal);
                System.out.println(sb.toString());
                lastBytesWritten = totalBytesWrittenLocal;
                counter += period;
            }
        }, 0, period);

        long begin = System.nanoTime();
        for (int i = 0; i < numPages; i++) {
            for (FileChannel channel : fileChannels) {
                byteBuffer.rewind();
                while (byteBuffer.hasRemaining()) {
                    totalBytesWriten += channel.write(byteBuffer);
                }
            }
        }
        for (int i = 0; i < files.length; i++) {
            fileChannels[i].force(false);
            fileChannels[i].close();
        }

        long end = System.nanoTime();
        System.out.println("Finish write in " + (end - begin) / 1000000 + " ms");
        timer.cancel();
        System.exit(0);
    }

}
