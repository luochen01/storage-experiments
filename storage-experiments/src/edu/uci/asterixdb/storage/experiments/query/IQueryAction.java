package edu.uci.asterixdb.storage.experiments.query;

public interface IQueryAction {

    public QueryResult run();

    public String getParameter();

}
