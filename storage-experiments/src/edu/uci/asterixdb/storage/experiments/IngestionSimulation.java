package edu.uci.asterixdb.storage.experiments;

public class IngestionSimulation {

    private int totalRecords;

    private final int RecordSize = 300;

    private final double updateRatio = 0.01;

    private final double insertRatio = 1 - updateRatio;

    private final int GB = 1024 * 1024 * 1024;

    private final int ComponentSize = GB;

    private final int memory = (int) (1.5 * GB);

    private final int PageSize = 128 * 1024;

    // each record needs 0.0003s
    private final double ConstIngestionCost = 0.0003;

    private final double DiskIoCost = 0.01;

    private final int MaxTime = 6 * 3600;

    public int getNumDiskComponents() {
        return (int) Math.ceil((double) totalRecords * RecordSize / ComponentSize);
    }

    public double getIngestionCost() {
        int workingMem = getWorkingMemory();
        if (workingMem < memory) {
            return ConstIngestionCost;
        } else {
            return ConstIngestionCost
                    + (updateRatio + 0.01 * getNumDiskComponents() * DiskIoCost) * (1 - (double) memory / workingMem);
        }
    }

    public int getWorkingMemory() {
        return 0;
    }

    public void run() {
        int time = 1;
        while (time++ < MaxTime) {
            double ingestCost = getIngestionCost();
            int ingestedRecord = (int) (1 / ingestCost);
            totalRecords += ingestedRecord;
            if (time % 60 == 0) {
                System.out.println(totalRecords);
            }
        }
    }

    public static void main(String[] args) {
        IngestionSimulation simulation = new IngestionSimulation();
        simulation.run();
    }

}
