package edu.uci.asterixdb.storage.experiments.query;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;

public abstract class AggregateQueryAction extends QueryAction {

    protected String batchMemory;

    public AggregateQueryAction(String dataverse, String dataset, Runnable preRun) {
        this(dataverse, dataset, preRun, null);
    }

    public AggregateQueryAction(String dataverse, String dataset, Runnable preRun, String batchMemory) {
        super(dataverse, dataset, preRun);
        this.batchMemory = batchMemory;
    }

    protected String getSetMemoryStatement() {
        if (batchMemory == null || batchMemory.isEmpty()) {
            return "";
        }
        return "SET `compiler.batchmemory` " + '"' + batchMemory + '"' + ";\n";
    }

    protected QueryResult toResult(String result) {
        System.out.println(result);
        JSONObject obj = new JSONObject(result);
        Object count = null;

        JSONArray resultArray = obj.getJSONArray("results");
        JSONObject resultObj = resultArray.optJSONObject(0);
        if (resultObj != null) {
            count = resultObj.opt("$1");
        }

        String time = obj.getJSONObject("metrics").getString("executionTime");

        String formattedTime = AsterixUtil.formatTime(time);

        return new QueryResult(getParameter(), count, formattedTime);
    }

}
