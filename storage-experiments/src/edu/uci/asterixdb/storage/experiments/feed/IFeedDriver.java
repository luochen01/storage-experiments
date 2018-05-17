package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;

public interface IFeedDriver {
    public String getNextTweet() throws IOException;

    public boolean isNewTweet();
}
