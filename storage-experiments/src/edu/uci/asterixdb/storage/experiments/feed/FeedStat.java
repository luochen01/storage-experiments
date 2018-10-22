package edu.uci.asterixdb.storage.experiments.feed;

public class FeedStat {

    public volatile long totalBytes = 0;

    public volatile long totalRecords = 0;

    public volatile long insertRecords = 0;

    public volatile long updateRecords = 0;

    public static FeedStat sum(FeedSocketAdapterClient[] clients) {
        FeedStat result = new FeedStat();
        for (FeedSocketAdapterClient client : clients) {
            result.totalBytes += client.stat.totalBytes;
            result.totalRecords += client.stat.totalRecords;
            result.insertRecords += client.stat.insertRecords;
            result.updateRecords += client.stat.updateRecords;
        }
        return result;
    }

    public void update(FeedStat stat) {
        this.totalBytes = stat.totalBytes;
        this.totalRecords = stat.totalRecords;
        this.insertRecords = stat.insertRecords;
        this.updateRecords = stat.updateRecords;
    }

}
