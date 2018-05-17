package edu.uci.asterixdb.storage.experiments.feed;

public class FileFeedRunner extends Thread {

    private final FeedSocketAdapterClient client;

    private final IFeedDriver driver;

    public FileFeedRunner(FeedSocketAdapterClient client, IFeedDriver driver) {
        this.client = client;
        this.driver = driver;
    }

    @Override
    public void run() {
        String str = null;
        try {
            while ((str = driver.getNextTweet()) != null) {
                client.ingest(str, driver.isNewTweet());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}