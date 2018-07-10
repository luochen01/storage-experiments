package edu.uci.asterixdb.storage.experiments.flowcontrol;

class Operation {
    public double totalCapacity;
    public double currentCapacity;
    public int cycles;

    public Operation(double totalCapacity) {
        this.totalCapacity = totalCapacity;
        this.currentCapacity = 0;
        this.cycles = 0;
    }

    public boolean isCompleted() {
        return currentCapacity >= totalCapacity || Math.abs(currentCapacity - totalCapacity) < 0.00001;
    }

    public void progress(double capacity) {
        currentCapacity = Math.min(currentCapacity + capacity, totalCapacity);
        cycles++;
    }

    public void reset() {
        this.currentCapacity = 0;
        this.cycles = 0;
        this.totalCapacity = 0;
    }

}

class FlushOperation extends Operation {

    public FlushOperation(double totalCapacity) {
        super(totalCapacity);
    }

    public void reset(double totalCapacity) {
        super.reset();
        this.totalCapacity = totalCapacity;
    }

}

class MergeOperation extends Operation {
    public final double[] components;

    public MergeOperation(double[] components) {
        super(getTotal(components));
        this.components = components;
    }

    private static double getTotal(double[] capacities) {
        double total = 0;
        for (double t : capacities) {
            total += t;
        }
        return total;
    }
}
