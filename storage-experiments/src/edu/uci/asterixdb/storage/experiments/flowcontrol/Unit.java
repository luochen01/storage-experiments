package edu.uci.asterixdb.storage.experiments.flowcontrol;

class Unit {
    public Operation operation;
    public boolean blocked = false;
    public final double maxCapacity;

    public Unit(double maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public Operation getOperation() {
        return operation;
    }

    public double getMaxCapacity() {
        return maxCapacity;
    }

    protected void reset() {
        this.operation = null;
        this.blocked = false;
    }

}

class FlushUnit extends Unit {
    public final double flushCapacity;
    public double capacity;

    public FlushUnit(double maxCapacity, double flushCapacity) {
        super(maxCapacity);
        this.flushCapacity = flushCapacity;
        this.capacity = 0;
    }

    @Override
    public FlushOperation getOperation() {
        return (FlushOperation) operation;
    }

    double remainingCapacity() {
        return maxCapacity - capacity;
    }

    protected void reset(double capacity) {
        super.reset();
        this.capacity = capacity;
    }
}

class MergeUnit extends Unit {
    public final int level;
    public final double[] components;
    public Unit blockedUnit;
    public final int maxNumComponents;
    public int numComponents = 0;

    public MergeUnit(int level, int maxNumComponents, double maxCapacity) {
        super(maxCapacity);
        this.level = level;
        this.maxNumComponents = maxNumComponents;
        this.components = new double[maxNumComponents];
    }

    @Override
    public MergeOperation getOperation() {
        return (MergeOperation) operation;
    }

    public boolean isFull() {
        return numComponents >= maxNumComponents;
    }

    public void setBlockedUnit(Unit unit) {
        assert this.blockedUnit == null;
        unit.blocked = true;
        this.blockedUnit = unit;
    }

    public void unsetBlockedUnit() {
        assert this.blockedUnit != null;
        this.blockedUnit.blocked = false;
        this.blockedUnit = null;
    }

    public void reset(double[] components) {
        super.reset();
        this.blockedUnit = null;
        this.numComponents = components.length;
        for (int i = 0; i < components.length; i++) {
            this.components[i] = components[i];
        }
    }

    public void addComponent(double component) {
        this.components[numComponents++] = component;
    }

}
