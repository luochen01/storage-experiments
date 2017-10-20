package edu.uci.asterixdb.storage.experiments.query;

import java.util.List;

public interface IQueryResultFormatter {
    public String format(List<QueryResult> results);
}
