package edu.uci.asterixdb.storage.experiments.lsm.simulator;

public class LSMExperimentUtil {

    public static String generateCountQuery(String dataverse, String dataset) {
        String query = String.format(
                "set `nopkindexcount` 'true'; set `compiler.readaheadmemory` '4MB';select count(*) from %s.%s;",
                dataverse, dataset);
        return query;
    }

}
