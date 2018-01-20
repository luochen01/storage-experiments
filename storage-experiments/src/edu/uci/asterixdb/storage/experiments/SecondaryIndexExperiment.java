package edu.uci.asterixdb.storage.experiments;

import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.Twitter;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildCorrelatedRandom;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildCorrelatedSequential;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildCountyMemoryExperiment;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildIndexOnly;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildNoneRandom;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildNoneSequential;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildPrefixRandom;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildPrefixSequential;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildStateMemoryExperiment;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildValidationCorrelatedRandom;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildValidationCorrelatedSequential;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildValidationIndexOnly;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.buildValidationPrefixRandom;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.ds_tweet_b_s;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.ds_tweet_n_r;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.ds_tweet_n_s;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.ds_tweet_p_r;
import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.getCleanCacheAction;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.uci.asterixdb.storage.experiments.index.query.IQueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.index.query.QueryGroup;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryResultFormatter;

public class SecondaryIndexExperiment {

    private final List<QueryGroup> groups = new ArrayList<>();

    private final IQueryResultFormatter formatter = new QueryResultFormatter();

    private final String basePath = "/home/cluo8/experiment";

    public SecondaryIndexExperiment() {
        //buildBatchExperiments();
        buildValidationExperiments();
        // buildValidationIndexOnlyExperiments();
    }

    private void buildValidationExperiments() {
        Runnable clearCacheNoneRandom = getCleanCacheAction(Twitter, ds_tweet_n_r);
        Runnable clearCacheNoneSequential = getCleanCacheAction(Twitter, ds_tweet_n_s);

        groups.add(buildNoneSequential(clearCacheNoneRandom));
        groups.add(buildNoneRandom(clearCacheNoneSequential));
        groups.add(buildValidationPrefixRandom(clearCacheNoneSequential));
        groups.add(buildValidationCorrelatedRandom(clearCacheNoneSequential));
        groups.add(buildValidationCorrelatedSequential(clearCacheNoneSequential));
    }

    private void buildValidationIndexOnlyExperiments() {
        Runnable clearCacheNoneSequential = getCleanCacheAction(Twitter, ds_tweet_n_s);

        groups.add(buildIndexOnly(clearCacheNoneSequential));
        groups.add(buildValidationIndexOnly(clearCacheNoneSequential));
        //groups.add(buildPrefixRandom(clearCacheNoneSequential));
    }

    private void buildBatchExperiments() {
        Runnable clearCache = getCleanCacheAction(Twitter, ds_tweet_b_s);

        groups.add(buildPrefixSequential(clearCache));
        groups.add(buildPrefixRandom(clearCache));
        groups.add(buildCorrelatedSequential(clearCache));
        groups.add(buildCorrelatedRandom(clearCache));
        groups.add(buildCountyMemoryExperiment(Twitter, ds_tweet_p_r, 6037, clearCache));
        groups.add(buildStateMemoryExperiment(Twitter, ds_tweet_p_r, 6, clearCache));
    }

    public void run() {
        long time = System.currentTimeMillis();
        for (QueryGroup group : groups) {
            String path = basePath + File.separator + group.getName() + "-" + time;
            List<QueryResult> results = group.run();
            QueryUtil.outputQueryResults(results, path);
        }
    }

    public static void main(String[] args) throws Exception {
        URI endpoint = new URI("http://sensorium-22.ics.uci.edu:19002/query/service");
        //URI endpoint = new URI("http://localhost:19002/query/service");
        QueryUtil.init(endpoint);
        SecondaryIndexExperiment experiment = new SecondaryIndexExperiment();
        experiment.run();
    }

}
