package edu.uci.asterixdb.storage.experiments.query;

import java.util.List;

public class QueryResultFormatter implements IQueryResultFormatter {

    @Override
    public String format(List<QueryResult> results) {
        StringBuilder sb = new StringBuilder();

        sb.append("parameter\tresult\ttime");
        sb.append(System.lineSeparator());

        for (QueryResult result : results) {
            sb.append(result);
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

}