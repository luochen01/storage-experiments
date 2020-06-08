package edu.uci.asterixdb.storage.experiments.feed;

import org.apache.commons.lang3.mutable.MutableBoolean;

public class FileFeedRunner extends Thread {

    private static final int reconnectInterval = 1000;

    private final FeedSocketAdapterClient client;

    private final IFeedDriver driver;

    public FileFeedRunner(FeedSocketAdapterClient client, IFeedDriver driver) {
        this.client = client;
        this.driver = driver;
    }

    @Override
    public void run() {
        while (true) {
            long id = 0;
            try {
                MutableBoolean isNew = new MutableBoolean();
                id = driver.getNextId(isNew);
                client.ingest(id, isNew.isTrue());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Try to reconnect...");
                try {
                    Thread.sleep(reconnectInterval);
                    client.reconnect();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }

    }
}