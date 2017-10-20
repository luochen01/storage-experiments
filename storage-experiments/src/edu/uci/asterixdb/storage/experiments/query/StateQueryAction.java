package edu.uci.asterixdb.storage.experiments.query;

import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;

public class StateQueryAction extends AggregateQueryAction {

    public StateQueryAction(String dataverse, String dataset, int stateId, Runnable preRun) {
        super(dataverse, dataset, preRun);
        this.stateId = stateId;
    }

    protected final int stateId;

    @Override
    public String getParameter() {
        return "stateID=" + stateId;
    }

    @Override
    protected QueryResult runImpl() {
        String query = QueryGenerator.state(dataverse, dataset, stateId);
        System.out.println(query);
        try {
            return toResult(AsterixUtil.executeQuery(query));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

}
