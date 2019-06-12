package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Experiment performance with sequential and random I/Os
 *
 * @author luochen
 *
 */
public class SSDReadExperiment implements Runnable {

    @Option(name = "-dir", required = true)
    public String dir = "";

    @Option(name = "-page-size", required = true)
    public int pageSize = 4096;

    @Option(name = "-file-size")
    public long fileSize = 10l * 1024 * 1024 * 1024;

    @Option(name = "-threads")
    public int numThreads = 1;

    @Option(name = "-write")
    public boolean write = false;

    @Option(name = "-read-size")
    public long readSize = 20l * 1024 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        SSDReadExperiment experiment = new SSDReadExperiment(args);
        experiment.experiment();
    }

    public SSDReadExperiment(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
        System.out.println("dir " + dir);
        System.out.println("page size " + pageSize);
        System.out.println("file size " + fileSize);
        System.out.println("numThreads " + numThreads);
        System.out.println("write " + write);
        System.out.println("read size " + readSize);
    }

    public void experiment() throws Exception {
        if (write) {
            int numPages = (int) (fileSize / pageSize);
            // create 10 files
            File dirFile = new File(dir);
            for (int i = 0; i < 5; i++) {
                File file = new File(dirFile, "file-" + i);
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                FileChannel chanel = raf.getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                for (int j = 0; j < numPages; j++) {
                    ThreadLocalRandom.current().nextBytes(buffer.array());
                    chanel.write(buffer, (long) pageSize * j);
                }
                chanel.force(true);
                raf.close();
            }
        } else {
            // do read
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(this);
            }
            long begin = System.nanoTime();
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            long end = System.nanoTime();

            long durationMs = TimeUnit.NANOSECONDS.toMillis(end - begin);
            long throughputMB = readSize / durationMs / 1000 / 1024 / 1024;
            System.out.println("Duration " + durationMs + " read throughput " + throughputMB + " MB/s");
        }
    }

    public void run() {
        try {

            long readSize = this.readSize / numThreads;

            File[] files = new File(dir).listFiles();
            RandomAccessFile[] rafs = new RandomAccessFile[files.length];
            FileChannel[] channels = new FileChannel[files.length];
            for (int i = 0; i < files.length; i++) {
                rafs[i] = new RandomAccessFile(files[i], "r");
                channels[i] = rafs[i].getChannel();
            }
            int pagesPerFile = (int) (fileSize / pageSize);
            int readPages = (int) (readSize / pageSize);
            ByteBuffer page = ByteBuffer.allocate(pageSize);
            for (int i = 0; i < readPages; i++) {
                int fileId = ThreadLocalRandom.current().nextInt(files.length);
                int pageId = ThreadLocalRandom.current().nextInt(pagesPerFile);
                page.reset();
                channels[fileId].read(page, (long) pageId * pageSize);
            }

            for (int i = 0; i < rafs.length; i++) {
                rafs[i].close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
