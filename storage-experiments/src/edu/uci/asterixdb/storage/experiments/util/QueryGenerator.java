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

    public static String sid(String dataverse, String dataset, int minSID, int maxSID, boolean skipPkIndex) {
        String skip = String.format("set `compiler.skip.pk.index` \"%s\";", String.valueOf(skipPkIndex));
        String query = String.format("select count(*) from %s.%s where sid >= %d AND sid <= %d;", dataverse, dataset,
                minSID, maxSID);
        return skip + query;
    }

    public static String scan(String dataverse, String dataset, String memory) {
        String skip = String.format("set `compiler.readaheadmemory` \"%s\";", memory);
        String query = String.format("select count(*) from %s.%s where latitude<20;", dataverse, dataset);
        return skip + query;
    }

}
