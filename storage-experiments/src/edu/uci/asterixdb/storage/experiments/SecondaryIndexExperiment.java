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

    private final String ds_tweet = "ds_tweet";

    private final String ds_tweet_sequential = "ds_tweet_c";

    private final String ds_tweet_random = "ds_tweet_r";

    private final String ds_tweet_prefix_random = "ds_tweet_pr";

    private final URI endpoint;

    private final List<QueryGroup> groups = new ArrayList<>();

    private final IQueryResultFormatter formatter = new QueryResultFormatter();

    private final String basePath = "/home/cluo8/experiment";

    private final Runnable cleanCache = new Runnable() {
        @Override
        public void run() {
            String count = QueryGenerator.countQuery(Twitter, ds_tweet_s);
            try {
                System.out.println(count);
                AsterixUtil.executeQuery(count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public SecondaryIndexExperiment(URI endpoint) {
        this.endpoint = endpoint;

        QueryGroup prefix = new QueryGroup("prefix");
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 51820, cleanCache));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 26115, cleanCache));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 54061, cleanCache));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 25027, cleanCache));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 24033, cleanCache));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 48113, cleanCache));
        prefix.addAction(new CountyQueryAction(Twitter, ds_tweet, 6037, cleanCache));
        prefix.addAction(new StateQueryAction(Twitter, ds_tweet, 6, cleanCache));
        // groups.add(prefix);

        QueryGroup sequential = new QueryGroup("sequential");
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 51820, cleanCache));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 26115, cleanCache));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 54061, cleanCache));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 25027, cleanCache));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 24033, cleanCache));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 48113, cleanCache));
        sequential.addAction(new CountyQueryAction(Twitter, ds_tweet_sequential, 6037, cleanCache));
        sequential.addAction(new StateQueryAction(Twitter, ds_tweet_sequential, 6, cleanCache));
        // groups.add(sequential);

        QueryGroup random = new QueryGroup("random");
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 51820, cleanCache));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 26115, cleanCache));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 54061, cleanCache));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 25027, cleanCache));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 24033, cleanCache));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 48113, cleanCache));
        random.addAction(new CountyQueryAction(Twitter, ds_tweet_random, 6037, cleanCache));
        random.addAction(new StateQueryAction(Twitter, ds_tweet_random, 6, cleanCache));
        // groups.add(random);

        QueryGroup prefixRandom = new QueryGroup("prefix_random");

        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "256KB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "512KB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "1MB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "2MB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "4MB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "8MB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "16MB"));
        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache, "32MB"));


        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "256KB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "512KB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "1MB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "2MB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "4MB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "8MB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "16MB"));
        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache, "32MB"));

        //        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 26115, cleanCache));
        //        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 54061, cleanCache));
        //        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 25027, cleanCache));
        //        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 24033, cleanCache));
        //        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 48113, cleanCache));
        //        prefixRandom.addAction(new CountyQueryAction(Twitter, ds_tweet_prefix_random, 6037, cleanCache));
        //        prefixRandom.addAction(new StateQueryAction(Twitter, ds_tweet_prefix_random, 6, cleanCache));
        groups.add(prefixRandom);
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
