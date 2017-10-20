package edu.uci.asterixdb.storage.experiments.query;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;

public abstract class AggregateQueryAction extends QueryAction {

    public AggregateQueryAction(String dataverse, String dataset, Runnable preRun) {
        super(dataverse, dataset, preRun);
    }

    protected QueryResult toResult(String result) {

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
