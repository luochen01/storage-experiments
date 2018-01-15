package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.gen.TweetGenerator;

public class FileFeedDriver {

    public enum FeedMode {
        Sequential,
        Update
    }

    @Option(required = true, name = "-u", aliases = "--url", usage = "url of the feed adapter")
    private String url;

    @Option(required = true, name = "-p", aliases = "--port", usage = "port of the feed socket")
    private String ports;

    @Option(name = "-l", aliases = "--log-path", usage = "path of the log file")
    private String logPath;

    @Option(name = "-d", aliases = "--duration", usage = "maximum duration of this experiment in seconds")
    private long duration = 0;

    @Option(name = "-update", aliases = "--update", usage = "update ratio")
    private double updateRatio = 0.0d;

    @Option(name = "-period", aliases = "--period", usage = "period of time to show info (in seconds)")
    private int period = 1;

    @Option(name = "-t", aliases = "--total", usage = "total records of this experiment")
    private long totalRecords = 0L;

    @Option(name = "-v", aliases = "--v", usage = "versions of each record")
    private double versions = 0;

    @Option(name = "-s", aliases = "--start", usage = "the start key range")
    private int startRange = 0;

    @Option(name = "-m", aliases = "--mode", usage = "the feed mode. validation options: sequential, update")
    private FeedMode mode = FeedMode.Update;

    private long endRange = 0;

    private final FeedSocketAdapterClient client;

    private final FeedReporter reporter;

    private final TweetGenerator gen;

    public static void main(String[] args) throws Exception {
        FileFeedDriver driver = new FileFeedDriver(args);
        driver.start();
    }

    public FileFeedDriver(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        if (mode == FeedMode.Update) {
            endRange = (long) (startRange + totalRecords / versions);
        }

        printConf();

        gen = new TweetGenerator(mode, updateRatio, startRange, endRange);

        client = new FeedSocketAdapterClient(url, Integer.valueOf(ports));
        reporter = new FeedReporter(this, client, period, logPath);
    }

    private void printConf() {
        System.out.println("FeedMode: " + mode);
        System.out.println("UpdateRatio: " + updateRatio);
        System.out.println("Duration: " + duration);
        System.out.println("TotalRecords: " + totalRecords);
        System.out.println("Versions: " + versions);
        System.out.println("StartRange: " + startRange);
        System.out.println("EndRange: " + endRange);
        System.out.println("LogPath: " + logPath);

    }

    public String getTweet() {
        return gen.getNextTweet();
    }

    public void start() throws InterruptedException, IOException {
        try {
            client.initialize();
            Thread thread = new FileFeedRunner(client, this);
            thread.start();
            reporter.start();

            long startTime = System.currentTimeMillis();

            while (!stop(startTime)) {
                Thread.sleep(1000);
            }

            reporter.close();
        } finally {
            System.exit(0);
        }
    }

    private boolean stop(long startTime) {
        if (duration > 0 && (System.currentTimeMillis() - startTime) >= duration * 1000) {
            return true;
        }
        if (client.stat.totalRecords >= totalRecords) {
            return true;
        }
        return false;
    }

}