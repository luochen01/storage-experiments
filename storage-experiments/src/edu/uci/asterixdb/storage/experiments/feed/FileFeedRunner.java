package edu.uci.asterixdb.storage.experiments.feed;

import org.apache.commons.lang3.mutable.MutableBoolean;

public class FileFeedRunner extends Thread {

    private final FeedSocketAdapterClient client;

    private final IFeedDriver driver;

    public FileFeedRunner(FeedSocketAdapterClient client, IFeedDriver driver) {
        this.client = client;
        this.driver = driver;
    }

    @Override
    public void run() {
        long id = 0;
        try {
            MutableBoolean isNew = new MutableBoolean();
            while (true) {
                id = driver.getNextId(isNew);
                client.ingest(id, isNew.isTrue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}