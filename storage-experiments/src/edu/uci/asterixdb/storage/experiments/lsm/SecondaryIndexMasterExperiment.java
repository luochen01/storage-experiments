package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class SecondaryIndexMasterExperiment {

    public static final String Twitter = "twitter";
    public static final String dataset = "ds_tweet";

    // 100k
    public static final int sidRange = 100000;

    @Option(name = "-dv", aliases = "--dataverse", usage = "the dataverse name", required = true)
    public String dataverseName;

    @Option(name = "-s", aliases = "--selectivity", usage = "query selectivity", required = true)
    public double selectivity;

    @Option(name = "-n", aliases = "--num", usage = "the number of queries", required = true)
    public int numQueries;

    @Option(name = "-o", aliases = "--output", usage = "output path", required = true)
    public String outputPath;

    @Option(name = "-nobatch")
    public boolean noBatch = false;

    private final Random rand = new Random(System.currentTimeMillis());

    public SecondaryIndexMasterExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        System.out.println("Dataverse: " + dataverseName);
        System.out.println("Selectivity: : " + selectivity);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Output path: " + outputPath);
        System.out.println("No Batch: " + noBatch);

    }

    public void run() throws Exception {
        PrintWriter writer = new PrintWriter(new File(outputPath));
        writer.println("seq\ttime\tresult");
        int queryRange = (int) (sidRange * selectivity);
        for (int i = 1; i <= numQueries; i++) {
            int beginRange = rand.nextInt(sidRange - queryRange);
            int endRange = beginRange + queryRange - 1;
            String query = generateSecondaryIndexQuery(beginRange, endRange);
            QueryResult result = QueryUtil.executeQuery("default", query);
            writer.println(i + "\t" + result.time + "\t" + result.result);
            System.out.println(i + "\t" + result.time + "\t" + result.result);
        }
        writer.flush();
        writer.close();
    }

    private String generateSecondaryIndexQuery(int beginRange, int endRange) {
        String noIndexOnly = "set `compiler.indexonly` 'false';";
        String nobatch = noBatch ? "set `no.batch` 'true';" : "";
        String query = String.format("select count(*) from %s.%s where sid>=%d AND sid<=%d;", dataverseName, dataset,
                beginRange, endRange);
        return noIndexOnly + nobatch + query;
    }

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        SecondaryIndexMasterExperiment expr = new SecondaryIndexMasterExperiment(args);
        expr.run();
    }

}
