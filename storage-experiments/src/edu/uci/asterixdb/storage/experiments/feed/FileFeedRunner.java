package edu.uci.asterixdb.storage.experiments.feed;

import java.util.concurrent.ArrayBlockingQueue;

class Entry {
    final long key;
    final boolean newRecord;

    public Entry(long key, boolean newRecord) {
        this.key = key;
        this.newRecord = newRecord;
    }
}

public class FileFeedRunner extends Thread {

    private final FeedSocketAdapterClient client;

    private final ArrayBlockingQueue<Entry> queue = new ArrayBlockingQueue<>(1024);

    public FileFeedRunner(FeedSocketAdapterClient client) {
        this.client = client;
    }

    public void put(long key, boolean newRecord) throws InterruptedException {
        queue.put(new Entry(key, newRecord));
    }

    @Override
    public void run() {
        try {
            while (true) {
                Entry e = queue.take();
                client.ingest(e.key, e.newRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}