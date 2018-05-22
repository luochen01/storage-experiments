package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.gen.IRecordGenerator;
import edu.uci.asterixdb.storage.experiments.feed.gen.KVGenerator;
import edu.uci.asterixdb.storage.experiments.feed.gen.RecordFileReader;
import edu.uci.asterixdb.storage.experiments.feed.gen.TweetGenerator;
import edu.uci.asterixdb.storage.experiments.util.ZipfianGenerator;

public class FileFeedDriver implements IFeedDriver {

    public enum FeedMode {
        Sequential,
        Random
    }

    public enum UpdateDistribution {
        UNIFORM,
        ZIPF,
    }

    public enum DataType {
        TWEET,
        KV
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

    @Option(name = "-theta", aliases = "--theta", usage = "the parameter for the zipfian generator. Default: 0.99")
    public double theta = ZipfianGenerator.ZIPFIAN_CONSTANT;

    @Option(name = "-input", aliases = "--input", usage = "the path of the input file")
    public String inputFile = null;

    @Option(name = "-type", aliases = "--type", usage = "data type (tweet or kv)")
    public DataType dataType = DataType.TWEET;

    @Option(name = "-size", aliases = "--size", usage = "record size (in bytes)")
    public int recordSize = 500;

    private final FeedSocketAdapterClient client;

    private final FeedReporter reporter;

    private final IRecordGenerator gen;

    public static void main(String[] args) throws Exception {
        FileFeedDriver driver = new FileFeedDriver(args);
        driver.start();
    }

    public FileFeedDriver(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        if (inputFile != null) {
            gen = new RecordFileReader(inputFile);
        } else {
            if (dataType == DataType.TWEET) {
                gen = new TweetGenerator(mode, distribution, theta, updateRatio, startRange, sidRange, 500);
            } else {
                gen = new KVGenerator(mode, distribution, theta, updateRatio, startRange, recordSize);
            }
        }
        client = new FeedSocketAdapterClient(url, Integer.valueOf(ports));
        reporter = new FeedReporter(client, period, logPath);
        printConf();
    }

    private void printConf() throws IOException {
        reporter.writeLine("FeedMode: " + mode);
        reporter.writeLine("UpdateDistribution: " + distribution);
        reporter.writeLine("UpdateRatio: " + updateRatio);
        reporter.writeLine("Duration: " + duration);
        reporter.writeLine("TotalRecords: " + totalRecords);
        reporter.writeLine("StartRange: " + startRange);
        reporter.writeLine("LogPath: " + logPath);
        reporter.writeLine("Theta: " + theta);
    }

    @Override
    public String getNextTweet() throws IOException {
        return gen.getNext();
    }

    @Override
    public boolean isNewTweet() {
        return gen.isNewRecord();
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