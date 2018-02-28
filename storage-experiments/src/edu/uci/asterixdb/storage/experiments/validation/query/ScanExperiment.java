package edu.uci.asterixdb.storage.experiments.validation.query;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class ScanExperiment {

    private final String dataverse = "twitter_$v";

    private final String dataset = "ds_tweet";

    private final String basePath = "~/experiment/";

    private final String[] memories = new String[] { "128KB", "256KB", "512KB", "1MB", "2MB", "4MB", "8MB", "16MB" };

    public void run() throws Exception {
        long ts = System.currentTimeMillis();
        List<QueryResult> results = new ArrayList<>();
        for (String memory : memories) {
            String query = QueryGenerator.scan(dataverse, dataset, memory);
            QueryResult result = QueryUtil.executeQuery(memory, query);
            results.add(result);
        }

        String path = basePath + File.separator + "scan-" + ts;
        QueryUtil.outputQueryResults(results, path);

    }

    public static void main(String[] args) throws Exception {
        URI endpoint = new URI("http://localhost:19002/query/service");
        //URI endpoint = new URI("http://sensorium-22.ics.uci.edu:19002/query/service");
        QueryUtil.init(endpoint);
        ScanExperiment experiment = new ScanExperiment();
        experiment.run();

    }

}
