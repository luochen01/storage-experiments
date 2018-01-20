package edu.uci.asterixdb.storage.experiments.index.query;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;

public interface IQueryAction {

    public QueryResult run();

    public String getParameter();

}
