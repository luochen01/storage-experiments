package edu.uci.asterixdb.storage.experiments.util;

public class QueryGenerator {

    public static String countQuery(String dataverse, String dataset) {
        return String.format("select count(*) from %s.%s;", dataverse, dataset);

    }

    public static String county(String dataverse, String dataset, int countyID) {
        return String.format("select count(*) from %s.%s where geo_tag.countyID = %d;", dataverse, dataset, countyID);
    }

    public static String state(String dataverse, String dataset, int stateID) {
        return String.format("select count(*) from %s.%s where geo_tag.stateID = %d;", dataverse, dataset, stateID);
    }

}
