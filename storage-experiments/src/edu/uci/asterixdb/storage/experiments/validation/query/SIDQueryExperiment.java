package edu.uci.asterixdb.storage.experiments.validation.query;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class SIDQueryExperiment {

    private static final int totalRecords = 10 * 1000000;

    //private int[] versions = new int[] { 1, 5, 10, 20 };

    private int[] versions = new int[] { 100 };

    private int queryRange = 1000;

    private int numQueries = 100;

    private final String dataverse = "twitter_";

    private final String dataset = "ds_tweet";

    private final String basePath = ".";

    public void run() throws Exception {
        long ts = System.currentTimeMillis();
        for (int i = 0; i < versions.length; i++) {
            int version = versions[i];
            int range = totalRecords / version;
            List<QueryResult> results = run(version, range, false);

            String path = basePath + File.separator + version + "-" + "pk" + "-" + ts;
            QueryUtil.outputQueryResults(results, path);
        }

        for (int i = 0; i < versions.length; i++) {
            int version = versions[i];
            int range = totalRecords / version;
            List<QueryResult> results = run(version, range, true);
            String path = basePath + File.separator + version + "-" + "direct" + "-" + ts;
            QueryUtil.outputQueryResults(results, path);
        }
    }

    private List<QueryResult> run(int version, int range, boolean skipPkIndex) throws Exception {
        RandomQueryGenerator gen =
                new RandomQueryGenerator(dataverse + version, dataset, 0, range, queryRange, skipPkIndex);
        List<QueryResult> results = new ArrayList<>();
        for (int i = 0; i < numQueries; i++) {
            String query = gen.next();
            QueryResult result = QueryUtil.executeQuery(String.valueOf(i), query);
            results.add(result);
        }
        return results;
    }

    public static void main(String[] args) throws Exception {
        URI endpoint = new URI("http://localhost:19002/query/service");
        QueryUtil.init(endpoint);
        SIDQueryExperiment experiment = new SIDQueryExperiment();
        experiment.run();

    }

}
