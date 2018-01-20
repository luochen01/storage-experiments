package edu.uci.asterixdb.storage.experiments.index.query;

import java.util.List;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;

public interface IQueryResultFormatter {
    public String format(List<QueryResult> results);
}
