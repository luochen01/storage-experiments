package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.gen.TweetGenerator;

public class FileFeedDriver {

    @Option(required = true, name = "-u", aliases = "--url", usage = "url of the feed adapter")
    private String url;

    @Option(required = true, name = "-p", aliases = "--port", usage = "port of the feed socket")
    private String ports;

    @Option(name = "-l", aliases = "--log-path", usage = "path of the log file")
    private String logPath;

    @Option(name = "-d", aliases = "--duration", usage = "maximum duration of this experiment in seconds")
    private long duration = 3650;

    @Option(name = "-update", aliases = "--update", usage = "update ratio")
    private double update_ratio = 0.0d;

    @Option(name="-period", aliases="--period", usage="period of time to show info (in seconds)")
    private int period = 1;

    private final List<FeedSocketAdapterClient> clients;

    private final FeedReporter reporter;

    private final TweetGenerator gen;

    public static void main(String[] args) throws Exception {
        FileFeedDriver driver = new FileFeedDriver(args);
        driver.start();
    }

    public FileFeedDriver(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        Map<String, String> conf = new HashMap<>();
        conf.put(TweetGenerator.KEY_UPDATE_RATIO, String.valueOf(update_ratio));
        gen = new TweetGenerator(conf);

        String[] portStrs = ports.split(",");
        clients = new ArrayList<>();
        for (int i = 0; i < portStrs.length; i++) {
            FeedSocketAdapterClient client = new FeedSocketAdapterClient(url, Integer.valueOf(portStrs[i]));
            client.initialize();
            clients.add(client);
        }
        reporter = new FeedReporter(this, clients, period, logPath);
    }

    public String getTweet() {
        return gen.getNextTweet();
    }

    public void start() throws InterruptedException, IOException {
        try {
            for (FeedSocketAdapterClient client : clients) {
                Thread thread = new FileFeedRunner(client, this);
                thread.start();
            }
            reporter.start();

            Thread.sleep(duration * 1000);
            reporter.close();
        } finally {
            System.exit(0);
        }

    }

}