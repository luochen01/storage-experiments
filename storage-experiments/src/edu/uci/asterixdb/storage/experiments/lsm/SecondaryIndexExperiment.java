package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class SecondaryIndexExperiment {

    public static final String Validation_NoRepair_0 = "twitter_validation_norepair_1";
    public static final String Validation_NoRepair_05 = "twitter_validation_norepair_5";

    public static final String Antimatter_0 = "twitter_antimatter_1";
    public static final String Antimatter_05 = "twitter_antimatter_5";

    public static final String Validation_0 = "twitter_validation_1";
    public static final String Validation_05 = "twitter_validation_5";

    public static final String dataset = "ds_tweet";

    // 100k
    public static final int sidRange = 100000;

    @Option(name = "-dv", aliases = "--dataverse", usage = "the dataverse name", required = true)
    public String dataverseName;

    @Option(name = "-cdv", aliases = "--cleandataverse", usage = "the dataverse name of clean cache")
    public String cleanCacheDataverse;

    @Option(name = "-s", aliases = "--selectivity", usage = "query selectivity", required = true)
    public double selectivity;

    @Option(name = "-n", aliases = "--num", usage = "the number of queries", required = true)
    public int numQueries;

    @Option(name = "-o", aliases = "--output", usage = "output path", required = true)
    public String outputPath;

    @Option(name = "-skippk", aliases = "--skippk", usage = "skip primary key index during validation", required = false)
    public boolean skipPkIndex = false;

    @Option(name = "-sortid", aliases = "--sortid", usage = "sort id", required = false)
    public boolean sortId = false;

    @Option(name = "-indexonly", aliases = "--indexonly", usage = "indexonly", required = false)
    public boolean indexOnly = false;

    @Option(name = "-b", aliases = "--batch", usage = "batch size (KB)", required = false)
    public int batchSizeKB = -1;

    @Option(name = "-nocid", aliases = "--nocid", usage = "no component id", required = false)
    public boolean noComponentId = false;

    private final Random rand = new Random(17);

    public SecondaryIndexExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        System.out.println("Dataverse: " + dataverseName);
        System.out.println("Selectivity: : " + selectivity);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Skip pk index in validation: " + skipPkIndex);
        System.out.println("Output path: " + outputPath);
        System.out.println("Clean cache dataverse: " + cleanCacheDataverse);
        System.out.println("Batch size: " + batchSizeKB);
        System.out.println("No Component Id: " + noComponentId);

    }

    public void run() throws Exception {
        if (cleanCacheDataverse != null) {
            String query = generateCountQuery(cleanCacheDataverse);
            QueryUtil.executeQuery("clean", query);
        }

        PrintWriter writer = new PrintWriter(new File(outputPath));
        writer.println("seq\ttime\tresult");
        int queryRange = (int) (sidRange * selectivity);
        for (int i = 1; i <= numQueries; i++) {
            int beginRange = rand.nextInt(sidRange - queryRange);
            int endRange = beginRange + queryRange - 1;
            String query = generateSecondaryIndexQuery(beginRange, endRange, skipPkIndex);
            QueryResult result = QueryUtil.executeQuery("default", query);
            writer.println(i + "\t" + result.time + "\t" + result.result);
        }
        writer.flush();
        writer.close();
    }

    private String generateSecondaryIndexQuery(int beginRange, int endRange, boolean skipPkIndex) {
        String skip = String.format("set `compiler.skip.pkindex` '%s';", String.valueOf(skipPkIndex));
        String indexOnly = String.format("set `noindexonly` '%s';", String.valueOf(!this.indexOnly));
        String batch = batchSizeKB >= 0 ? String.format("set `compiler.batchmemory` '%dKB';", batchSizeKB) : "";
        String nocid = noComponentId ? "set `nocomponentid` 'true'" : "";
        String query = sortId
                ? String.format(
                        "select count(*) from (select id from %s.%s where sid>=%d AND sid<=%d order by id) tmp;",
                        dataverseName, dataset, beginRange, endRange)
                : String.format("select count(*) from %s.%s where sid>=%d AND sid<=%d;", dataverseName, dataset,
                        beginRange, endRange);
        return skip + indexOnly + nocid + batch + query;
    }

    private String generateCountQuery(String dataverse) {
        String query = String.format(
                "set `nopkindexcount` 'true';set `compiler.readaheadmemory` '4MB';select count(*) from %s.%s",
                dataverse, dataset);
        return query;
    }

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        SecondaryIndexExperiment expr = new SecondaryIndexExperiment(args);
        expr.run();
    }

}
