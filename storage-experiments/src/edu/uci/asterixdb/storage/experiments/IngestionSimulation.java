package edu.uci.asterixdb.storage.experiments;

public class IngestionSimulation {

    private long totalRecords;

    private final int RecordSize = 100;

    private final double updateRatio = 0.05;

    private final double insertRatio = 1 - updateRatio;

    private final int GB = 1024 * 1024 * 1024;

    private final int ComponentSize = GB;

    private final int memory = (GB);

    private final int PageSize = 128 * 1024;

    private final double ConstIngestionCost = (double) 600 / 1882310;

    private final double DiskIoCost = 0.01;

    private final int MaxTime = 6 * 3600;

    private final double BloomFilterFP = 0.01;

    public int getNumDiskComponents() {
        return (int) Math.ceil(getTotalData() / ComponentSize);
    }

    public double getIngestionCost() {
        long workingMem = getWorkingMemory();
        if (workingMem < memory) {
            return ConstIngestionCost;
        } else {
            double diskCost = (updateRatio * (1 + BloomFilterFP * getNumDiskComponents() / 2))
                    * (1 - (double) memory / workingMem) * DiskIoCost;
            return ConstIngestionCost + diskCost;
        }
    }

    public long getWorkingMemory() {
        return getTotalData();
    }

    public long getTotalData() {
        return (long) (totalRecords * RecordSize * (1 - updateRatio));
    }

    public void run() {
        int time = 1;
        while (time++ < MaxTime) {
            double ingestCost = getIngestionCost();
            int ingestedRecord = (int) (1 / ingestCost);
            totalRecords += ingestedRecord;
            if (time % 60 == 0) {
                //  System.out.println(time + "    " + totalRecords + "    " + ingestedRecord * 60);
                System.out.println(totalRecords);
            }
        }
    }

    public static void main(String[] args) {
        IngestionSimulation simulation = new IngestionSimulation();
        simulation.run();
    }

}
