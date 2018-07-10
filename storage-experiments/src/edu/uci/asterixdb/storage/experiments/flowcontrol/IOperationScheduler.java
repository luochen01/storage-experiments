package edu.uci.asterixdb.storage.experiments.flowcontrol;

public interface IOperationScheduler {
    void reset();

    void initialize(ILSMSimulator simulator);

    int getMaxNumComponents();

    int getNumRunningOperations();

    boolean isFlushable(FlushUnit mergeUnit);

    FlushOperation scheduleFlushOperation(FlushUnit mergeUnit);

    void completeFlushOperation(FlushUnit flushUnit);

    boolean isMergeable(MergeUnit mergeUnit);

    MergeOperation scheduleMergeOperation(MergeUnit mergeUnit);

    void completeMergeOperation(MergeUnit mergeUnit, MergeUnit nextUnit);

}
