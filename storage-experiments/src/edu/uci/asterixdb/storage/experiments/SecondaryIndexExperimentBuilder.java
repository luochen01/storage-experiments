package edu.uci.asterixdb.storage.experiments;

import edu.uci.asterixdb.storage.experiments.query.CountyQueryAction;
import edu.uci.asterixdb.storage.experiments.query.QueryGroup;
import edu.uci.asterixdb.storage.experiments.query.StateQueryAction;
import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;

public class SecondaryIndexExperimentBuilder {
    public static final String Twitter = "twitter";

    public static final String ds_tweet_b_s = "ds_tweet_s";

    public static final String ds_tweet_b_r = "ds_tweet_rs";

    public static final String ds_tweet_p_s = "ds_tweet";

    public static final String ds_tweet_p_r = "ds_tweet_pr";

    public static final String ds_tweet_c_s = "ds_tweet_c";

    public static final String ds_tweet_c_r = "ds_tweet_r";

    public static final String ds_tweet_v_p_r = "ds_tweet_v_p_r";

    public static final String ds_tweet_v_c_r = "ds_tweet_v_c_r";

    public static final String ds_tweet_v_c_s = "ds_tweet_v_c_s";

    public static final String ds_tweet_n_s = ds_tweet_p_s;

    public static final String ds_tweet_n_r = ds_tweet_c_r;

    public static final String default_memory_size = "1MB";

    public static QueryGroup buildBulkloadSequentialExperiment(Runnable action) {
        return buildExperiment("bulkload-sequential", Twitter, ds_tweet_b_s, action);
    }

    public static QueryGroup buildBulkloadRandomExperiment(Runnable action) {
        return buildExperiment("bulkload-random", Twitter, ds_tweet_b_r, action);
    }

    public static QueryGroup buildCorrelatedSequential(Runnable action) {
        return buildExperiment("correlated-sequential", Twitter, ds_tweet_c_s, action);
    }

    public static QueryGroup buildCorrelatedRandom(Runnable action) {
        return buildExperiment("correlated-random", Twitter, ds_tweet_c_r, action);
    }

    public static QueryGroup buildPrefixRandom(Runnable action) {
        return buildExperiment("prefix-random", Twitter, ds_tweet_p_r, action);
    }

    public static QueryGroup buildPrefixSequential(Runnable action) {
        return buildExperiment("prefix-sequential", Twitter, ds_tweet_p_s, action);
    }

    public static QueryGroup buildNoneSequential(Runnable action) {
        return buildExperiment("none-sequential", Twitter, ds_tweet_n_s, action);
    }

    public static QueryGroup buildNoneRandom(Runnable action) {
        return buildExperiment("none-random", Twitter, ds_tweet_n_r, action);
    }

    public static QueryGroup buildValidationCorrelatedRandom(Runnable action) {
        return buildExperiment("validation-correlated-random", Twitter, ds_tweet_v_c_r, action);
    }

    public static QueryGroup buildValidationCorrelatedSequential(Runnable action) {
        return buildExperiment("validation-correlated-sequential", Twitter, ds_tweet_v_c_s, action);
    }

    public static QueryGroup buildValidationPrefixRandom(Runnable action) {
        return buildExperiment("validation-prefix-random", Twitter, ds_tweet_v_p_r, action);
    }

    public static QueryGroup buildValidationIndexOnly(Runnable action) {
        return buildIndexOnlyExperiment("validation-index-only", Twitter, ds_tweet_v_p_r, action);
    }

    public static QueryGroup buildIndexOnly(Runnable action) {
        return buildIndexOnlyExperiment("index-only", Twitter, ds_tweet_p_r, action);
    }

    public static QueryGroup buildCountyMemoryExperiment(String dataverse, String dataset, int countyID,
            Runnable before) {
        QueryGroup group = new QueryGroup("memory-county");
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "128KB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "256KB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "512KB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "1MB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "2MB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "4MB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "8MB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "16MB"));
        group.addAction(new CountyQueryAction(dataverse, dataset, countyID, before, "32MB"));
        return group;
    }

    public static QueryGroup buildStateMemoryExperiment(String dataverse, String dataset, int stateID,
            Runnable before) {
        QueryGroup group = new QueryGroup("memory-state");
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "128KB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "256KB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "512KB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "1MB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "2MB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "4MB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "8MB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "16MB"));
        group.addAction(new StateQueryAction(dataverse, dataset, stateID, before, "32MB"));
        return group;
    }

    private static QueryGroup buildExperiment(String name, String dataverse, String dataset, Runnable action) {
        QueryGroup group = new QueryGroup(name);
        group.addAction(new CountyQueryAction(dataverse, dataset, 51820, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 26115, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 54061, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 25027, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 24033, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 48113, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 6037, action, default_memory_size));
        group.addAction(new StateQueryAction(dataverse, dataset, 6, action, default_memory_size));
        return group;
    }

    private static QueryGroup buildIndexOnlyExperiment(String name, String dataverse, String dataset, Runnable action) {
        QueryGroup group = new QueryGroup(name);
        group.addAction(new CountyQueryAction(dataverse, dataset, 51820, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 51820, null, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 26115, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 26115, null, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 54061, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 54061, null, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 25027, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 25027, null, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 24033, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 24033, null, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 48113, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 48113, null, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 6037, action, default_memory_size));
        group.addAction(new CountyQueryAction(dataverse, dataset, 6037, null, default_memory_size));
        group.addAction(new StateQueryAction(dataverse, dataset, 6, action, default_memory_size));
        group.addAction(new StateQueryAction(dataverse, dataset, 6, null, default_memory_size));
        return group;
    }

    public static Runnable getCleanCacheAction(String dataverse, String dataset) {
        Runnable action = new Runnable() {
            @Override
            public void run() {
                String count = QueryGenerator.countQuery(dataverse, dataset);
                try {
                    System.out.println(count);
                    String result = AsterixUtil.executeQuery(count);
                    System.out.println(result);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        return action;
    }

}