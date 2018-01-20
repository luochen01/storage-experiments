package edu.uci.asterixdb.storage.experiments.index.query;

import edu.uci.asterixdb.storage.experiments.util.QueryUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;

public class CountyQueryAction extends AggregateQueryAction {

    public CountyQueryAction(String dataverse, String dataset, int countyId, Runnable preRun) {
        this(dataverse, dataset, countyId, preRun, null);
    }

    public CountyQueryAction(String dataverse, String dataset, int countyId, Runnable preRun, String memory) {
        super(dataverse, dataset, preRun, memory);
        this.countyId = countyId;
    }

    protected final int countyId;

    @Override
    public String getParameter() {
        return "countyID=" + countyId;
    }

    @Override
    protected QueryResult runImpl() {
        String query = QueryGenerator.county(dataverse, dataset, countyId);
        query = getSetMemoryStatement() + query;
        System.out.println(query);
        try {
            return QueryUtil.executeQuery(getParameter(), query);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

}
