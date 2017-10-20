package edu.uci.asterixdb.storage.experiments;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MemoryReadExperiments {

    private final int pageSize;

    private final int numPages;

    private final int numZones;

    private final List<byte[]> buffers;

    private final int numReads;

    private final int recordLength = 100;

    private final int pagesPerZone;

    public MemoryReadExperiments(int totalBytes, int pageSize, int numZones, int numReads) {
        this.pageSize = pageSize;
        this.numPages = totalBytes / pageSize;
        this.numZones = numZones;
        this.numReads = numReads;
        this.pagesPerZone = numPages / numZones;

        this.buffers = new ArrayList<>(numZones);
        for (int i = 0; i < this.numZones; i++) {
            byte[] buffer = new byte[pagesPerZone * pageSize];
            for (int j = 0; j < buffer.length; j++) {
                buffer[j] = (byte) j;
            }

            this.buffers.add(new byte[pagesPerZone * pageSize]);
        }
    }

    public void run() {
        sequentialRead();
        randomRead();
    }

    private void sequentialRead() {
        byte[] buffer = buffers.get(0);
        long begin = System.currentTimeMillis();
        byte[] recordBuffer = new byte[recordLength];
        for (int i = 0; i < numReads; i++) {
            int offset = i * recordLength;
            int numZone = offset / buffer.length;
            offset = offset % buffer.length;
            System.arraycopy(buffers.get(numZone), offset > recordLength ? offset - recordLength : offset, recordBuffer,
                    0, recordLength);
        }
        long end = System.currentTimeMillis();

        System.out.println("Sequential read finish in " + (end - begin) + " ms");

    }

    private void randomRead() {
        Random rand = new Random(System.currentTimeMillis());
        long begin = System.currentTimeMillis();
        byte[] pageBuffer = new byte[pageSize];
        for (int i = 0; i < numReads; i++) {
            int page = rand.nextInt(numPages - 1);
            int numZone = page / pagesPerZone;
            page = page % pagesPerZone;
            System.arraycopy(buffers.get(numZone), page, pageBuffer, 0, recordLength);
        }
        long end = System.currentTimeMillis();

        System.out.println("Random read finish in " + (end - begin) + " ms");
    }

    public static void main(String[] args) {
        // 1GB
        int totalBytes = 1 * 1024 * 1024 * 1024;
        // 128 KB
        int pageSize = 128 * 1024;
        // 64 zones
        int numZones = 64;
        // 1M read
        int numReads = 1024 * 1024;
        MemoryReadExperiments exp = new MemoryReadExperiments(totalBytes, pageSize, numZones, numReads);
        exp.run();

    }

}
