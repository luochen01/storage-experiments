package edu.uci.asterixdb.storage.experiments.feed.gen;

import java.io.IOException;

public interface IRecordGenerator {
    public String getNext() throws IOException;

    public boolean isNewRecord();
}
