package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.feed.gen.DataGenerator.TweetMessage;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class RandomFilterExperiment {

    public static final String dataset = "ds_tweet";

    public static final DateTime minDate = new DateTime(2010, 1, 1, 0, 0, 1);

    public static final DateTime maxDate = new DateTime(2012, 7, 10, 0, 0, 1);

    @Option(name = "-dv", aliases = "--dataverse", usage = "the dataverse name", required = true)
    public String dataverseName;

    @Option(name = "-cdv", aliases = "--cleandataverse", usage = "the dataverse name of clean cache")
    public String cleanCacheDataverse;

    @Option(name = "-d", aliases = "--duration", usage = "duration (hour)")
    public int duration;

    public long durationMilli;

    @Option(name = "-n", aliases = "--num", usage = "the number of queries", required = true)
    public int numQueries;

    @Option(name = "-o", aliases = "--output", usage = "output path", required = true)
    public String outputPath;

    @Option(name = "-ra", aliases = "--readahead", usage = "readAheadMemorySize(KB)")
    public long readAheadKB = 4 * 1024;

    @Option(name = "-partial", aliases = "--partial", usage = "partial filters")
    public boolean partial;

    private final Random rand = new Random(System.currentTimeMillis());

    public RandomFilterExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        System.out.println("Dataverse: " + dataverseName);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Output path: " + outputPath);
        System.out.println("Clean cache dataverse: " + cleanCacheDataverse);
        System.out.println("Read ahead memory size (KB): " + readAheadKB);
        System.out.println("duration (hour): " + duration);

        durationMilli = TimeUnit.HOURS.toMillis(duration);

    }

    public void run() throws Exception {
        if (cleanCacheDataverse != null) {
            String query = LSMExperimentUtil.generateCountQuery(cleanCacheDataverse, dataset);
            QueryUtil.executeQuery("clean", query);
        }
        PrintWriter writer = new PrintWriter(new File(outputPath));
        writer.println("seq\ttime\tresult");
        for (int i = 1; i < numQueries; i++) {
            DateTime minValue = generateMinValue();
            DateTime maxValue = minValue.plus(durationMilli);
            String query = generateFilterQuery(minValue, maxValue);
            QueryResult result = QueryUtil.executeQuery("default", query);
            writer.println(i + "\t" + result.time + "\t" + result.result);
        }
        writer.flush();
        writer.close();
    }

    private DateTime generateMinValue() {
        long begin = minDate.getMillis();
        long end = maxDate.getMillis() - durationMilli;
        long value = begin + Math.abs(rand.nextLong() % (end - begin));
        return new DateTime(value);
    }

    private String generateFilterQuery(DateTime minValue, DateTime maxValue) {
        String batch = String.format("set `compiler.readaheadmemory` '%dKB';", readAheadKB);
        String query = partial
                ? String.format("select count(*) from %s.%s where created_at>=datetime(%s);", dataverseName, dataset,
                        TweetMessage.getDateTimeString(minValue), TweetMessage.getDateTimeString(maxValue))
                : String.format(
                        "select count(*) from %s.%s where created_at>=datetime(%s) AND created_at<=datetime(%s);",
                        dataverseName, dataset, TweetMessage.getDateTimeString(minValue),
                        TweetMessage.getDateTimeString(maxValue));
        return batch + query;
    }

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        RandomFilterExperiment expr = new RandomFilterExperiment(args);
        expr.run();
    }

}
