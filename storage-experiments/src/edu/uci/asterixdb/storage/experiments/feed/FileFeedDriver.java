package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.gen.TweetGenerator;

public class FileFeedDriver {

    public enum FeedMode {
        Sequential,
        Random
    }

    public enum UpdateDistribution {
        UNIFORM,
        ZIPF_TIME,
        ZIPF_ITEM
    }

    @Option(required = true, name = "-u", aliases = "--url", usage = "url of the feed adapter")
    public String url;

    @Option(required = true, name = "-p", aliases = "--port", usage = "port of the feed socket")
    public String ports;

    @Option(name = "-l", aliases = "--log-path", usage = "path of the log file")
    public String logPath;

    @Option(name = "-d", aliases = "--duration", usage = "maximum duration of this experiment in seconds")
    public long duration = 0;

    @Option(name = "-update", aliases = "--update", usage = "update ratio")
    public double updateRatio = 0.0d;

    @Option(name = "-period", aliases = "--period", usage = "period of time to show info (in seconds)")
    public int period = 1;

    @Option(name = "-t", aliases = "--total", usage = "total records of this experiment")
    public long totalRecords = Long.MAX_VALUE;

    @Option(name = "-s", aliases = "--start", usage = "the start key range")
    public int startRange = 0;

    @Option(name = "-r", aliases = "--sidrange", usage = "the sid total range")
    // 100K
    public int sidRange = 100000;

    @Option(name = "-m", aliases = "--mode", usage = "the feed mode. validation options: sequential, random")
    public FeedMode mode = FeedMode.Random;

    @Option(name = "-dist", aliases = "--distribution", usage = "the distribution of updates. validate operations: sequential, update")
    public UpdateDistribution distribution = UpdateDistribution.UNIFORM;

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

        printConf();

        gen = new TweetGenerator(mode, distribution, updateRatio, startRange, sidRange);

        client = new FeedSocketAdapterClient(url, Integer.valueOf(ports));
        reporter = new FeedReporter(this, client, period, logPath);
    }

    private void printConf() {
        System.out.println("FeedMode: " + mode);
        System.out.println("UpdateRatio: " + updateRatio);
        System.out.println("Duration: " + duration);
        System.out.println("TotalRecords: " + totalRecords);
        System.out.println("StartRange: " + startRange);
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
        } catch (Exception e) {
            e.printStackTrace();
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