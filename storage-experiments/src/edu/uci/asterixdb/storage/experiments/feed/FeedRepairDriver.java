package edu.uci.asterixdb.storage.experiments.feed;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.FeedMode;
import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.UpdateDistribution;
import edu.uci.asterixdb.storage.experiments.feed.gen.IRecordGenerator;
import edu.uci.asterixdb.storage.experiments.feed.gen.IdGenerator;
import edu.uci.asterixdb.storage.experiments.feed.gen.TweetGenerator;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;
import edu.uci.asterixdb.storage.experiments.util.ZipfianGenerator;

public class FeedRepairDriver implements IFeedDriver {

    private static final Logger LOGGER = LogManager.getLogger(FeedRepairDriver.class);

    public enum RepairMethod {
        Dataset,
        Index
    }

    @Option(required = true, name = "-dv", aliases = "--dv", usage = "dataverse name")
    public String dataverse;

    @Option(required = true, name = "-repair", aliases = "--repair", usage = "repair method")
    public RepairMethod repairMethod;

    @Option(name = "-compact", aliases = "--compact", usage = "compact during repair")
    public boolean compact = false;

    @Option(name = "-rf", aliases = "--repairfreq", usage = "repair frequency")
    public long repairFrequency = 1000000;

    @Option(name = "-rw", aliases = "--repairwait", usage = "wait time before start repair (seconds)")
    public long repairWait = 5;

    @Option(required = true, name = "-u", aliases = "--url", usage = "url of the feed adapter")
    public String url;

    @Option(required = true, name = "-p", aliases = "--port", usage = "port of the feed socket")
    public String ports;

    @Option(name = "-l", aliases = "--log-path", usage = "path of the log file")
    public String logPath;

    @Option(name = "-rl", aliases = "--repair-log-path", usage = "path of the repair log file")
    public String repairLogPath;

    @Option(name = "-update", aliases = "--update", usage = "update ratio")
    public double updateRatio = 0.0d;

    @Option(name = "-period", aliases = "--period", usage = "period of time to show info (in seconds)")
    public int period = 1;

    @Option(required = true, name = "-t", aliases = "--total", usage = "total records of this experiment")
    public long totalRecords;

    @Option(name = "-s", aliases = "--start", usage = "the start key range")
    public int startRange = 0;

    @Option(name = "-r", aliases = "--sidrange", usage = "the sid total range")
    // 100K
    public int sidRange = 100000;

    @Option(name = "-size", aliases = "--size", usage = "the size of the record")
    public int recordSize = 500;

    @Option(name = "-m", aliases = "--mode", usage = "the feed mode. validation options: sequential, random")
    public FeedMode mode = FeedMode.Random;

    @Option(name = "-dist", aliases = "--distribution", usage = "the distribution of updates. validate operations: sequential, update")
    public UpdateDistribution distribution = UpdateDistribution.UNIFORM;

    @Option(name = "-theta", aliases = "--theta", usage = "the parameter for the zipfian generator. Default: 0.99")
    public double theta = ZipfianGenerator.ZIPFIAN_CONSTANT;

    @Option(name = "-parallel", aliases = "--parallel", usage = "repair paralleism ")
    public int parallelism = 1;

    private final FeedSocketAdapterClient client;

    private final FeedReporter reporter;

    private final IRecordGenerator gen;
    private final IdGenerator idGen;

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));

        FeedRepairDriver driver = new FeedRepairDriver(args);
        driver.start();
    }

    public FeedRepairDriver(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        this.repairWait = this.repairWait * 1000;

        idGen = IdGenerator.create(distribution, theta, startRange, updateRatio, mode.equals(FeedMode.Random));
        gen = new TweetGenerator(sidRange, recordSize);
        client = new FeedSocketAdapterClient(url, Integer.valueOf(ports), gen);
        reporter = new FeedReporter(new FeedSocketAdapterClient[] { client }, period, logPath);
        printConf();
    }

    private void printConf() throws IOException {
        reporter.writeLine("FeedMode: " + mode);
        reporter.writeLine("UpdateDistribution: " + distribution);
        reporter.writeLine("UpdateRatio: " + updateRatio);
        reporter.writeLine("TotalRecords: " + totalRecords);
        reporter.writeLine("StartRange: " + startRange);
        reporter.writeLine("LogPath: " + logPath);
        reporter.writeLine("Repair LogPath: " + repairLogPath);
        reporter.writeLine("RepairFrequency: " + repairFrequency);
        reporter.writeLine("RepairWait (ms): " + repairWait);
        reporter.writeLine("Dataverse: " + dataverse);
        reporter.writeLine("RepairMethod: " + repairMethod);
    }

    public String getRepairQuery() {
        String readAhead = "set `compiler.readaheadmemory` '4MB';";
        String parallel = "set `compiler.repair.parallelism` '" + parallelism + "';";
        String query = "";
        if (repairMethod == RepairMethod.Dataset) {
            query = String.format("repair dataset %s.ds_tweet %s;", dataverse, compact ? "compact" : "");
        } else {
            query = String.format("repair index %s.ds_tweet.`*` %s;", dataverse, compact ? "compact" : "");
        }
        return readAhead + parallel + query;
    }

    public void start() throws InterruptedException, IOException {
        try {
            client.initialize();
            reporter.start();
            BufferedWriter repairLogWritter = new BufferedWriter(new FileWriter(repairLogPath));
            long ingestedRecords = 0;
            while (ingestedRecords < totalRecords) {
                long id = idGen.next();
                boolean isNew = idGen.isNewId();
                client.ingest(id, isNew);
                ingestedRecords++;
                if (ingestedRecords % repairFrequency == 0) {
                    LOGGER.error("Prepare to start repair, sleep for {} ms...", repairWait);
                    Thread.sleep(repairWait);
                    String query = getRepairQuery();
                    QueryResult result = QueryUtil.executeQuery("repair", query);
                    repairLogWritter.write(ingestedRecords + "\t" + result.time + System.lineSeparator());
                }
            }

            repairLogWritter.close();
            reporter.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

}