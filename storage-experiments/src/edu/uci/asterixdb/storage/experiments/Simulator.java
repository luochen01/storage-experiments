package edu.uci.asterixdb.storage.experiments;

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
    final Simulator simulator;
    final double totalBytes;
    final int level;
    double mergedBytes;

    public MergeOp(Simulator simulator, double totalBytes, int level) {
        this.simulator = simulator;
        this.totalBytes = totalBytes;
        this.level = level;
    }

    @Override
    public double getRemainingTime() {
        return (totalBytes - mergedBytes) / (simulator.diskBandwidth / simulator.getNumMergeOperations());
    }
}

public class Simulator {

    private static int sizeRatio = 3;

    final double diskBandwidth = 1000;

    private final double memoryComponentSize = 1000;

    private static int maxNumOperations = 5;

    private final int numLevels = 5;

    private Level[] levels = new Level[numLevels];

    private List<Op> runningOps = new ArrayList<>();
    private int numMergeOps = 0;

    private final double maxTime = 24 * 3600 * 10;

    private int maxNumComponents = 0;

    private int numComponents = 0;

    private double componentArrivalTime = memoryComponentSize / (diskBandwidth / numLevels);

    public Simulator() {
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
            for (Op runningOp : runningOps) {
                update(runningOp, elapsedTime);
            }
            if (op instanceof NewComponentOp) {
                newComponentOp = (NewComponentOp) op;
                levels[0].components.add(0, memoryComponentSize);
                incNumComponents();

                newComponentOp.remainingTime = componentArrivalTime;
                runningOps.add(newComponentOp);
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

    private void update(Op op, double elapsedTime) {
        if (op instanceof NewComponentOp) {
            NewComponentOp newOp = (NewComponentOp) op;
            newOp.remainingTime -= elapsedTime;
        } else if (op instanceof MergeOp) {
            MergeOp mergeOp = (MergeOp) op;
            mergeOp.mergedBytes += elapsedTime * diskBandwidth / getNumMergeOperations();
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

        Simulator.sizeRatio = 3;
        for (int i = 1; i <= 5; i++) {
            Simulator.maxNumOperations = i;
            Simulator sim = new Simulator();
            sim.simulate();
            System.out.println("max operations: " + i + " max components " + sim.maxNumComponents);
        }

    }
}
