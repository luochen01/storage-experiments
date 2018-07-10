package edu.uci.asterixdb.storage.experiments.flowcontrol;

public interface ILSMSimulator {

    FlushUnit getFlushUnit();

    MergeUnit getMergeUnit(int level);

}
