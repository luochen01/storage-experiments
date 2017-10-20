package edu.uci.asterixdb.storage.experiments.query;

public class QueryResult {

    public final String parameter;

    public final Object result;

    public final String time;

    public QueryResult(String paramter, Object result, String time) {
        this.parameter = paramter;
        this.result = result;
        this.time = time;
    }

    @Override
    public String toString() {
        return parameter + "\t" + result + "\t" + time;
    }

}
