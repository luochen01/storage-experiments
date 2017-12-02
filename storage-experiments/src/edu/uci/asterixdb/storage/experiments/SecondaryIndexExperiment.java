package edu.uci.asterixdb.storage.experiments;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.uci.asterixdb.storage.experiments.query.CountyQueryAction;
import edu.uci.asterixdb.storage.experiments.query.IQueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.query.QueryGroup;
import edu.uci.asterixdb.storage.experiments.query.QueryResult;
import edu.uci.asterixdb.storage.experiments.query.QueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.query.StateQueryAction;
import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;

public class SecondaryIndexExperiment {

    private final String Twitter = "twitter";

    private final String ds_tweet_s = "ds_tweet_s";

    private final String ds_tweet_rs = "ds_tweet_rs";

    private final String ds_tweet = "ds_tweet";

    private final String ds_tweet_sequential = "ds_tweet_c";

    private final String ds_tweet_random = "ds_tweet_r";

    private final String ds_tweet_prefix_random = "ds_tweet_pr";

    private final URI endpoint;

    private final List<QueryGroup> groups = new ArrayList<>();

    private final IQueryResultFormatter formatter = new QueryResultFormatter();

    private final String basePath = "/home/cluo8/experiment";

    private final Runnable cleanCache_s = new Runnable() {
        @Override
        public void run() {
            String count = QueryGenerator.countQuery(Twitter, ds_tweet_s);
            try {
                System.out.println(count);
                String result = AsterixUtil.executeQuery(count);
                System.out.println(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    private final Runnable cleanCache_rs = new Runnable() {
        @Override
        public void run() {
            String count = QueryGenerator.countQuery(Twitter, ds_tweet_rs);
            try {
                System.out.println(count);
                String result = AsterixUtil.executeQuery(count);
                System.out.println(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public SecondaryIndexExperiment(URI endpoint) {
        this.endpoint = endpoint;

        QueryGroup bulkload = new QueryGroup("bulkload-sequential");
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 51820, cleanCache_rs));
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 26115, cleanCache_rs));
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 54061, cleanCache_rs));
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 25027, cleanCache_rs));
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 24033, cleanCache_rs));
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 48113, cleanCache_rs));
        bulkload.addAction(new CountyQueryAction(Twitter, ds_tweet_s, 6037, cleanCache_rs));
        bulkload.addAction(new StateQueryAction(Twitter, ds_tweet_s, 6, cleanCache_rs));
        groups.add(bulkload);

        QueryGroup bulkloadRandom = new QueryGroup("bulkload-random");
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 51820, cleanCache_s));
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 26115, cleanCache_s));
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 54061, cleanCache_s));
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 25027, cleanCache_s));
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 24033, cleanCache_s));
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 48113, cleanCache_s));
        bulkloadRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_rs, 6037, cleanCache_s));
        bulkloadRandom.addAction(new StateQueryAction(Twitter, ds_tweet_rs, 6, cleanCache_s));
        groups.add(bulkloadRandom);

        QueryGroup prefix = new QueryGroup("prefix-sequential");
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 51820, cleanCache_s));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 26115, cleanCache_s));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 54061, cleanCache_s));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 25027, cleanCache_s));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 24033, cleanCache_s));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 48113, cleanCache_s));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 6037, cleanCache_s));
        prefix.addAction(new StateQueryAction(Twitter, ds_tweet, 6, cleanCache_s));
        groups.add(prefix);

        QueryGroup sequential = new QueryGroup("correlated-sequential");
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 51820, cleanCache_s));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 26115, cleanCache_s));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 54061, cleanCache_s));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 25027, cleanCache_s));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 24033, cleanCache_s));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 48113, cleanCache_s));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 6037, cleanCache_s));
        sequential.addAction(new StateQueryAction(Twitter, ds_tweet_sequential, 6, cleanCache_s));
        groups.add(sequential);

        QueryGroup prefixRandom = new QueryGroup("prefix-random");
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 51820, cleanCache_s));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 26115, cleanCache_s));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 54061, cleanCache_s));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 25027, cleanCache_s));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 24033, cleanCache_s));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 48113, cleanCache_s));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s));
        groups.add(prefixRandom);

        QueryGroup random = new QueryGroup("correlated-random");
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 51820, cleanCache_s));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 26115, cleanCache_s));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 54061, cleanCache_s));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 25027, cleanCache_s));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 24033, cleanCache_s));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 48113, cleanCache_s));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 6037, cleanCache_s));
        random.addAction(new StateQueryAction(Twitter, ds_tweet_random, 6, cleanCache_s));
        groups.add(random);

        QueryGroup memory = new QueryGroup("memory");

        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "256KB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "512KB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "1MB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "2MB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "4MB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "8MB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "16MB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "32MB"));
        memory.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache_s, "64MB"));

        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "256KB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "512KB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "1MB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "2MB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "4MB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "8MB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "16MB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "32MB"));
        memory.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache_s, "64MB"));

        //groups.add(memory);
    }

    public void run() {
        long time = System.currentTimeMillis();
        for (QueryGroup group : groups) {
            List<QueryResult> results = group.run();
            String result = formatter.format(results);
            System.out.println(group.getName());
            System.out.println(result);
            try {
                FileWriter writer = new FileWriter(new File(basePath, group.getName() + "-" + time));
                writer.write(result);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws Exception {
        URI endpoint = new URI("http://sensorium-22.ics.uci.edu:19002/query/service");
        //URI endpoint = new URI("http://localhost:19002/query/service");
        AsterixUtil.init(endpoint);
        SecondaryIndexExperiment experiment = new SecondaryIndexExperiment(endpoint);
        experiment.run();
    }

}
