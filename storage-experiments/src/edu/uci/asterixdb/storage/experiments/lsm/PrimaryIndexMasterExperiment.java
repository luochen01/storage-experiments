package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class PrimaryIndexMasterExperiment {

    public static final String Twitter = "twitter";
    public static final String dataset = "ds_tweet";

    // 2billon
    public static final long pkRange = 2_000_000_000;

    @Option(name = "-dv", aliases = "--dataverse", usage = "the dataverse name", required = true)
    public String dataverseName;

    @Option(name = "-s", aliases = "--selectivity", usage = "query selectivity", required = true)
    public double selectivity;

    @Option(name = "-n", aliases = "--num", usage = "the number of queries", required = true)
    public int numQueries;

    @Option(name = "-o", aliases = "--output", usage = "output path", required = true)
    public String outputPath;

    private final Random rand = new Random(System.currentTimeMillis());

    public PrimaryIndexMasterExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        System.out.println("Dataverse: " + dataverseName);
        System.out.println("Selectivity: : " + selectivity);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Output path: " + outputPath);
    }

    public void run() throws Exception {
        PrintWriter writer = new PrintWriter(new File(outputPath));
        writer.println("seq\ttime\tresult");
        long queryRange = (long) (pkRange * selectivity);
        for (int i = 1; i <= numQueries; i++) {
            long beginRange = Math.abs(rand.nextLong()) % (pkRange - queryRange);
            long endRange = beginRange + queryRange - 1;
            String query = generatePrimaryIndexQuery(beginRange, endRange);
            QueryResult result = QueryUtil.executeQuery("default", query);
            writer.println(i + "\t" + result.time + "\t" + result.result);
            System.out.println(i + "\t" + result.time + "\t" + result.result);
        }
        writer.flush();
        writer.close();
    }

    private String generatePrimaryIndexQuery(long beginRange, long endRange) {
        String query = String.format("select count(*) from %s.%s where id>=%d AND id<=%d;", dataverseName, dataset,
                beginRange, endRange);
        return query;
    }

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        PrimaryIndexMasterExperiment expr = new PrimaryIndexMasterExperiment(args);
        expr.run();
    }

}
