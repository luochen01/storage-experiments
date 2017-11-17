package edu.uci.asterixdb.storage.experiments.feed;

import java.util.List;

public class FeedStat {

    public volatile long totalBytes = 0;

    public volatile long totalRecords = 0;

    public static FeedStat sum(List<FeedSocketAdapterClient> clients) {
        FeedStat result = new FeedStat();
        for (FeedSocketAdapterClient client : clients) {
            result.totalBytes += client.stat.totalBytes;
            result.totalRecords += client.stat.totalRecords;
        }
        return result;
    }

    public void update(FeedStat stat) {
        this.totalBytes = stat.totalBytes;
        this.totalRecords = stat.totalRecords;
    }

}
