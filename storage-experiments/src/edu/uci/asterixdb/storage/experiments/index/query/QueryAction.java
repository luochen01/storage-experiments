package edu.uci.asterixdb.storage.experiments.index.query;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;

public abstract class QueryAction implements IQueryAction {

    protected final String dataverse;

    protected final String dataset;

    protected final Runnable preRun;

    public QueryAction(String dataverse, String dataset) {
        this(dataverse, dataset, null);
    }

    public QueryAction(String dataverse, String dataset, Runnable preRun) {
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.preRun = preRun;
    }

    @Override
    public final QueryResult run() {
        if (preRun != null) {
            preRun.run();
        }
        System.out.println("Start new experiment " + getParameter());
        return runImpl();

    };

    protected abstract QueryResult runImpl();

}
