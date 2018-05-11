package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.joda.time.DateTime;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.gen.DataGenerator.TweetMessage;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class StaticFilterExperiment {

    public static final String dataset = "ds_tweet";

    public static final String dv_antimatter_1 = "twitter_antimatter_UNIFORM_1";
    public static final String dv_antimatter_5 = "twitter_antimatter_UNIFORM_5";
    public static final String dv_validation_1 = "twitter_validation_UNIFORM_1";
    public static final String dv_validation_5 = "twitter_validation_UNIFORM_5";

    public static final DateTime minTime = new DateTime(2010, 1, 1, 0, 0, 1);

    public static final Map<String, DateTime> maxTimes = new HashMap<>();
    static {
        maxTimes.put(dv_antimatter_1, new DateTime(2012, 7, 14, 7, 49, 11));
        maxTimes.put(dv_antimatter_5, new DateTime(2012, 7, 14, 23, 0, 35));
        maxTimes.put(dv_validation_1, new DateTime(2012, 7, 13, 17, 42, 2));
        maxTimes.put(dv_validation_5, new DateTime(2012, 7, 14, 23, 40, 9));
    }

    @Option(name = "-dv", aliases = "--dataverse", usage = "the dataverse name", required = true)
    public String dataverseName;

    @Option(name = "-cdv", aliases = "--cleandataverse", usage = "the dataverse name of clean cache")
    public String cleanCacheDataverse;

    @Option(name = "-d", aliases = "--day", usage = "the number of days")
    public int days;

    @Option(name = "-n", aliases = "--num", usage = "the number of queries", required = true)
    public int numQueries;

    @Option(name = "-o", aliases = "--output", usage = "output path", required = true)
    public String outputPath;

    @Option(name = "-ra", aliases = "--readahead", usage = "readAheadMemorySize(KB)")
    public long readAheadKB = 4 * 1024;

    public enum Mode {
        RECENT,
        HISTORY,
        HISTORY_PARTIAL
    }

    @Option(name = "-m", aliases = "--mode", usage = "mode: RECENT HISTORY", required = true)
    public Mode mode;

    private final Random rand = new Random(System.currentTimeMillis());

    public StaticFilterExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        System.out.println("Dataverse: " + dataverseName);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Output path: " + outputPath);
        System.out.println("Clean cache dataverse: " + cleanCacheDataverse);
        System.out.println("Read ahead memory size (KB): " + readAheadKB);
        System.out.println("days: " + days);
        System.out.println("mode: " + mode);
    }

    public void run() throws Exception {
        PrintWriter writer = new PrintWriter(new File(outputPath));
        writer.println("seq\ttime\tresult");

        DateTime maxTime = maxTimes.get(dataverseName);
        for (int i = 1; i < numQueries; i++) {
            if (cleanCacheDataverse != null) {
                String query = LSMExperimentUtil.generateCountQuery(cleanCacheDataverse, dataset);
                QueryUtil.executeQuery("clean", query);
            }
            DateTime minFilter, maxFilter = null;
            if (mode == Mode.RECENT) {
                minFilter = maxTime.minusDays(days);
            } else if (mode == Mode.HISTORY) {
                minFilter = minTime;
                maxFilter = minTime.plusDays(days);
            } else {
                minFilter = minTime;
            }
            String query = generateFilterQuery(minFilter, maxFilter);
            QueryResult result = QueryUtil.executeQuery("default", query);
            writer.println(i + "\t" + result.time + "\t" + result.result);
        }
        writer.flush();
        writer.close();
    }

    private String generateFilterQuery(DateTime minFilter, DateTime maxFilter) {
        String batch = String.format("set `compiler.readaheadmemory` '%dKB';", readAheadKB);

        StringBuilder query = new StringBuilder();
        query.append(batch);
        query.append("select count(*) from ");
        query.append(dataverseName);
        query.append(".");
        query.append(dataset);
        query.append(" where ");
        if (minFilter != null) {
            query.append(" created_at >= datetime(");
            query.append(TweetMessage.getDateTimeString(minFilter));
            query.append(") ");
        }
        if (maxFilter != null) {
            if (minFilter != null) {
                query.append(" AND ");
            }
            query.append(" created_at <= datetime(");
            query.append(TweetMessage.getDateTimeString(maxFilter));
            query.append(") ");
        }
        query.append(";");
        return query.toString();
    }

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        StaticFilterExperiment expr = new StaticFilterExperiment(args);
        expr.run();
    }

}
