package edu.uci.asterixdb.storage.experiments;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;

import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class FullTextQueryClient {
    public static final String RUNNING_REQUESTS = "http://localhost:19002/admin/requests/running/";
    public static URI endpoint;
    public static final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(10));

    public static final Random rand = new Random();

    public static void main(String[] args) throws Exception {
        endpoint = new URI("http://localhost:19002/query/service");
        QueryUtil.init(endpoint);

        for (int i = 0; i < 1000; i++) {
            System.out.println("Executing query " + i);
            execute();
            Thread.sleep(500);
        }

        System.exit(1);
    }

    public static void execute() throws Exception {
        String query = "select max(sid) from twitter.ds_tweet where ftcontains(message_text,['iphone','like'])";
        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        String clientId = UUID.randomUUID().toString();
        RequestBuilder builder = RequestBuilder.post(endpoint);
        builder.addParameter("statement", query);
        builder.addParameter("client_context_id", clientId);
        builder.setCharset(StandardCharsets.UTF_8);
        HttpUriRequest post = builder.build();

        Future<HttpResponse> future = executor.submit(new Callable<HttpResponse>() {
            @Override
            public HttpResponse call() throws Exception {
                return client.execute(post);
            }
        });
        while (!future.isDone()) {
            Thread.sleep(rand.nextInt(200) + 10);
            // cancel query
            RequestBuilder deleteBuilder = RequestBuilder.delete(RUNNING_REQUESTS);
            deleteBuilder.addParameter("client_context_id", clientId);
            deleteBuilder.setCharset(StandardCharsets.UTF_8);
            HttpUriRequest deleteRequest = deleteBuilder.build();
            HttpResponse deleteResponse = client.execute(deleteRequest);
            int rc = deleteResponse.getStatusLine().getStatusCode();
            if (rc == 200) {
                break;
            }
        }
        HttpResponse response = future.get();
        String queryResult = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
        System.out.println(queryResult);
    }
}
