package edu.uci.asterixdb.storage.experiments.greedy;

import java.util.ArrayList;
import java.util.List;

class Level {
    List<Double> components = new ArrayList<>();
    boolean merging;

    @Override
    public String toString() {
        return components.toString();
    }
}

abstract class Op {
    public abstract double getRemainingTime();
}

class NewComponentOp extends Op {
    double remainingTime;

    @Override
    public double getRemainingTime() {
        return remainingTime;
    }
}

class MergeOp extends Op {
    final SimulatorGreedy simulator;
    final double totalBytes;
    final int level;
    double mergedBytes;

    public MergeOp(SimulatorGreedy simulator, double totalBytes, int level) {
        this.simulator = simulator;
        this.totalBytes = totalBytes;
        this.level = level;
    }

    @Override
    public double getRemainingTime() {
        return (totalBytes - mergedBytes) / (simulator.diskBandwidth);
    }
}

public class SimulatorGreedy {

    private static int sizeRatio = 3;

    final double diskBandwidth = 1000;

    private final double memoryComponentSize = 1000;

    private static int maxNumOperations = 100;

    private final int numLevels = 5;

    private Level[] levels = new Level[numLevels];

    private List<Op> runningOps = new ArrayList<>();
    private int numMergeOps = 0;

    private final double maxTime = 24 * 3600 * 10;

    private int maxNumComponents = 0;

    private int numComponents = 0;

    private double componentArrivalTime = memoryComponentSize / (diskBandwidth / numLevels);

    public SimulatorGreedy() {
        for (int i = 0; i < numLevels; i++) {
            levels[i] = new Level();
        }
    }

    public void simulate() {
        NewComponentOp newComponentOp = new NewComponentOp();
        newComponentOp.remainingTime = componentArrivalTime;
        runningOps.add(newComponentOp);

        double currentTime = 0;
        while (currentTime < maxTime) {
            Op op = removeMin(runningOps);
            double elapsedTime = op.getRemainingTime();
            // update all events
            newComponentOp.remainingTime -= elapsedTime;

            if (op instanceof NewComponentOp) {
                newComponentOp = (NewComponentOp) op;
                levels[0].components.add(0, memoryComponentSize);
                incNumComponents();

                newComponentOp.remainingTime = componentArrivalTime;
                runningOps.add(newComponentOp);

                MergeOp activeMergeOp = findActiveMergeOp(runningOps);
                if (activeMergeOp != null) {
                    activeMergeOp.mergedBytes += elapsedTime * diskBandwidth;
                }
            } else if (op instanceof MergeOp) {
                MergeOp mergeOp = (MergeOp) op;
                numMergeOps--;
                Level level = levels[mergeOp.level];
                level.merging = false;
                level.components.subList(level.components.size() - sizeRatio, level.components.size()).clear();;
                if (mergeOp.level < numLevels - 1) {
                    levels[mergeOp.level + 1].components.add(0, mergeOp.totalBytes);
                    incNumComponents();
                }
                numComponents -= sizeRatio;
            }

            // schedule new merges
            for (int i = numLevels - 1; i >= 0; i--) {
                if (!levels[i].merging && levels[i].components.size() >= sizeRatio && numMergeOps < maxNumOperations) {
                    levels[i].merging = true;
                    MergeOp mergeOp = new MergeOp(this, levels[i].components.get(0) * sizeRatio, i);
                    runningOps.add(mergeOp);
                    numMergeOps++;
                }
            }
            currentTime += elapsedTime;
        }

    }

    private MergeOp findActiveMergeOp(List<Op> runningOps) {
        int minIndex = -1;
        for (int i = 0; i < runningOps.size(); i++) {
            if (!(runningOps.get(i) instanceof MergeOp)) {
                continue;
            }
            if (minIndex == -1 || runningOps.get(i).getRemainingTime() < runningOps.get(minIndex).getRemainingTime()) {
                minIndex = i;
            }
        }
        if (minIndex == -1) {
            return null;
        } else {
            return (MergeOp) runningOps.get(minIndex);
        }
    }

    private Op removeMin(List<Op> runningOps) {
        int minIndex = 0;
        for (int i = 1; i < runningOps.size(); i++) {
            if (runningOps.get(i).getRemainingTime() < runningOps.get(minIndex).getRemainingTime()) {
                minIndex = i;
            }
        }
        return runningOps.remove(minIndex);
    }

    private void incNumComponents() {
        numComponents++;
        maxNumComponents = Math.max(numComponents, maxNumComponents);
    }

    public int getNumMergeOperations() {
        return numMergeOps;
    }

    public static void main(String[] args) {

        SimulatorGreedy.sizeRatio = 3;
        SimulatorGreedy sim = new SimulatorGreedy();
        sim.simulate();
        System.out.println(" max components " + sim.maxNumComponents);

    }
}
