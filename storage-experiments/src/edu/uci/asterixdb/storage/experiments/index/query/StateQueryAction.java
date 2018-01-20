package edu.uci.asterixdb.storage.experiments.index.query;

import edu.uci.asterixdb.storage.experiments.util.QueryUtil;
import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;
import edu.uci.asterixdb.storage.experiments.util.QueryResult;

public class StateQueryAction extends AggregateQueryAction {

    public StateQueryAction(String dataverse, String dataset, int stateId, Runnable preRun) {
        this(dataverse, dataset, stateId, preRun, null);
    }

    public StateQueryAction(String dataverse, String dataset, int stateId, Runnable preRun, String memory) {
        super(dataverse, dataset, preRun, memory);
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
