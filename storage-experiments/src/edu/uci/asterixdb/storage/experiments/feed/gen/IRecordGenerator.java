package edu.uci.asterixdb.storage.experiments.feed.gen;

@FunctionalInterface
public interface IRecordGenerator {

    public String getRecord(long id);
}
