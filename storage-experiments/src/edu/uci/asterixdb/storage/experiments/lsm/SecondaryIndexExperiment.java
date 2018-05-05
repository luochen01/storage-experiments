package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;

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

    private final String dataverseName;

    private final String cleanCacheDataverse;

    private final double selectivity;

    private final int numQueries;

    private final String outputPath;

    private final boolean skipPkIndex;

    private final Random rand = new Random(17);

    public SecondaryIndexExperiment(String dataverseName, double selectivity, int numQueries, boolean skipPkIndex,
            String outputPath, String cleanCacheDataverse) {
        this.dataverseName = dataverseName;
        this.selectivity = selectivity;
        this.numQueries = numQueries;
        this.skipPkIndex = skipPkIndex;
        this.outputPath = outputPath;
        this.cleanCacheDataverse = cleanCacheDataverse;
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
        String skip = String.format("set `compiler.skip.pkindex` \"%s\";", String.valueOf(skipPkIndex));
        String query = String.format("set `noindexonly` 'true';select count(*) from %s.%s where sid>=%d AND sid<=%d;",
                dataverseName, dataset, beginRange, endRange);
        return skip + query;
    }

    private String generateCountQuery(String dataverse) {
        String query = String.format("set `compiler.readaheadmemory` '4MB'; set `noindexonly` 'true';select count(*) from %s.%s where latitude>0;", dataverse,
                dataset);
        return query;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: dataverse, selectivity, numQueries, skipPkIndex, outputPath, [clenaCacheDataverse]");
            return;
        }
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        String dataverse = args[0];
        double selectivity = Double.parseDouble(args[1]);
        int numQueries = Integer.parseInt(args[2]);
        boolean skipPkIndex = Boolean.parseBoolean(args[3]);
        String outputPath = args[4];
        String cleanCacheDataverse = null;
        if (args.length >= 6) {
            cleanCacheDataverse = args[5];
        }

        System.out.println("Dataverse: " + dataverse);
        System.out.println("Selectivity: : " + selectivity);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Skip pk index in validation: " + skipPkIndex);
        System.out.println("Output path: " + outputPath);
        System.out.println("Clean cache dataverse: " + cleanCacheDataverse);
        SecondaryIndexExperiment expr = new SecondaryIndexExperiment(dataverse, selectivity, numQueries, skipPkIndex,
                outputPath, cleanCacheDataverse);
        expr.run();
    }

}
