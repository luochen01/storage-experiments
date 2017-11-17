package edu.uci.asterixdb.storage.experiments.feed;

public class FileFeedRunner extends Thread {

    private final FeedSocketAdapterClient client;

    private final FileFeedDriver driver;

    public FileFeedRunner(FeedSocketAdapterClient client, FileFeedDriver driver) {
        this.client = client;
        this.driver = driver;
    }

    @Override
    public void run() {
        String str = null;
        try {
            while ((str = driver.getTweet()) != null) {
                client.ingest(str);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}