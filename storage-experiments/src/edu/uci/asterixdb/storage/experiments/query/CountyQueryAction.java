package edu.uci.asterixdb.storage.experiments.query;

import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;

public class CountyQueryAction extends AggregateQueryAction {

    public CountyQueryAction(String dataverse, String dataset, int countyId, Runnable preRun) {
        super(dataverse, dataset, preRun);
        this.countyId = countyId;
    }

    protected final int countyId;

    @Override
    public String getParameter() {
        return "conutyId=" + countyId;
    }

    @Override
    protected QueryResult runImpl() {
        String query = QueryGenerator.county(dataverse, dataset, countyId);
        System.out.println(query);
        try {
            return toResult(AsterixUtil.executeQuery(query));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

}
