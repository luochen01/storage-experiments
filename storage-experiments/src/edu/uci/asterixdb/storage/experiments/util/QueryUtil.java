package edu.uci.asterixdb.storage.experiments.util;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class QueryUtil {

    private static final Logger LOGGER = LogManager.getLogger(QueryUtil.class);

    public static URI DEFAULT_ENDPOINT = null;

    private static DecimalFormat TimeFormatter = new DecimalFormat("#.00");

    public static void init(URI endpoint) {
        DEFAULT_ENDPOINT = endpoint;
    }

    public static QueryResult executeQuery(String key, String query) throws Exception {
        if (DEFAULT_ENDPOINT == null) {
            throw new IllegalStateException("Default endpoint has not been initialized");
        }
        return executeQuery(key, query, DEFAULT_ENDPOINT);
    }

    public static QueryResult executeQuery(String key, String query, URI endpoint) throws Exception {
        LOGGER.warn(query);

        RequestBuilder builder = RequestBuilder.post(endpoint);
        builder.addParameter("statement", query);
        builder.setCharset(StandardCharsets.UTF_8);
        HttpUriRequest post = builder.build();

        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();

        HttpResponse response = client.execute(post);
        String queryResult = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());

        LOGGER.warn(queryResult);

        JSONObject obj = new JSONObject(queryResult);

        JSONArray resultArray = obj.optJSONArray("results");

        String time = obj.getJSONObject("metrics").getString("executionTime");
        String formattedTime = QueryUtil.formatTime(time);

        return new QueryResult(key, resultArray, formattedTime);
    }

    public static String formatTime(String time) {
        double value = 0.0d;
        if (time.endsWith("ms")) {
            value = Double.valueOf(time.substring(0, time.length() - 2));
        } else if (time.endsWith("s")) {
            value = Double.valueOf(time.substring(0, time.length() - 1)) * 1000;
        } else {
            throw new UnsupportedOperationException("Unknown time format " + time);
        }
        return TimeFormatter.format(value);

    }
}
