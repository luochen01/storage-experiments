package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;

import org.apache.commons.lang3.mutable.MutableBoolean;

public interface IFeedDriver {
    public long getNextId(MutableBoolean isNew) throws IOException;

}
