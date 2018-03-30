package edu.uci.asterixdb.storage.experiments;

import static edu.uci.asterixdb.storage.experiments.SecondaryIndexExperimentBuilder.*;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.uci.asterixdb.storage.experiments.index.query.IQueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.index.query.QueryAction;
import edu.uci.asterixdb.storage.experiments.index.query.QueryGroup;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryResultFormatter;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

class ScanAction extends QueryAction {
    private final String limit;

    public ScanAction(String dataverse, String dataset, String limit, Runnable before) {
        super(dataverse, dataset, before);
        this.limit = limit;
    }

    @Override
    public String getParameter() {
        return limit;
    }

    @Override
    protected QueryResult runImpl() {
        String query = String.format("select value t from %s.%s t limit %s;", dataverse, dataset, limit);
        System.out.println(query);
        try {
            return QueryUtil.executeQuery(getParameter(), query);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}

class LookupAction extends QueryAction {
    private final String limit;
    private final String sidRange;

    public LookupAction(String dataverse, String dataset, String sidRange, String limit, Runnable before) {
        super(dataverse, dataset, before);
        this.limit = limit;
        this.sidRange = sidRange;
    }

    @Override
    public String getParameter() {
        return limit;
    }

    @Override
    protected QueryResult runImpl() {
        String query =
                String.format("select * from %s.%s where sid < %s limit %s;", dataverse, dataset, sidRange, limit);
        System.out.println(query);
        try {
            return QueryUtil.executeQuery(getParameter(), query);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}

public class LimitExperiment {

    private final List<QueryGroup> groups = new ArrayList<>();

    private final IQueryResultFormatter formatter = new QueryResultFormatter();

    private final String basePath = "/tmp/";

    public LimitExperiment() {
        scanExperiments();
        lookupExperiments("lookup-1%", "1000");
        lookupExperiments("lookup-10%", "10000");
    }

    private void scanExperiments() {
        Runnable clearCache = getCleanCacheAction(Twitter, ds_tweet);
        QueryGroup group = new QueryGroup("scan-limit");
        group.addAction(new ScanAction(Twitter, ds_tweet, "10", clearCache));
        group.addAction(new ScanAction(Twitter, ds_tweet, "100", clearCache));
        group.addAction(new ScanAction(Twitter, ds_tweet, "1000", clearCache));
        group.addAction(new ScanAction(Twitter, ds_tweet, "10000", clearCache));
        group.addAction(new ScanAction(Twitter, ds_tweet, "100000", clearCache));
        groups.add(group);
    }

    private void lookupExperiments(String name, String sidRange) {
        Runnable clearCache = getCleanCacheAction(Twitter, ds_tweet);
        QueryGroup group = new QueryGroup(name);
        group.addAction(new LookupAction(Twitter, ds_tweet, sidRange, "10", clearCache));
        group.addAction(new LookupAction(Twitter, ds_tweet, sidRange, "100", clearCache));
        group.addAction(new LookupAction(Twitter, ds_tweet, sidRange, "1000", clearCache));
        group.addAction(new LookupAction(Twitter, ds_tweet, sidRange, "10000", clearCache));
        group.addAction(new LookupAction(Twitter, ds_tweet, sidRange, "100000", clearCache));
        groups.add(group);
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
        //URI endpoint = new URI("http://sensorium-22.ics.uci.edu:19002/query/service");
        URI endpoint = new URI("http://localhost:19002/query/service");
        QueryUtil.init(endpoint);
        LimitExperiment experiment = new LimitExperiment();
        experiment.run();
    }

}
