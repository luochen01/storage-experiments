package edu.uci.asterixdb.storage.experiments;

import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.uci.asterixdb.storage.experiments.query.IQueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.query.QueryGroup;
import edu.uci.asterixdb.storage.experiments.query.QueryResult;
import edu.uci.asterixdb.storage.experiments.query.QueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;

public class SecondaryIndexExperiment {

    private final List<QueryGroup> groups = new ArrayList<>();

    private final IQueryResultFormatter formatter = new QueryResultFormatter();

    private final String basePath = "/home/cluo8/experiment";

    public SecondaryIndexExperiment() {
        buildValidationExperiments();
    }

    private void buildValidationExperiments() {
        Runnable clearCacheNone = getCleanCacheAction(Twitter, ds_tweet_v_c_r);
        Runnable clearCacheValidation = getCleanCacheAction(Twitter, ds_tweet_n_s);

        groups.add(buildNoneSequential(clearCacheNone));
        groups.add(buildNoneRandom(clearCacheNone));
        groups.add(buildValidationPrefixRandom(clearCacheValidation));
        groups.add(buildValidationCorrelatedRandom(clearCacheValidation));

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
        SecondaryIndexExperiment experiment = new SecondaryIndexExperiment();
        experiment.run();
    }

}
