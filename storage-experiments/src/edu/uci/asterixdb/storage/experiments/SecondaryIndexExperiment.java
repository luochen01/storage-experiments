package edu.uci.asterixdb.storage.experiments;

import java.net.URI;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import edu.uci.asterixdb.storage.experiments.util.AsterixUtil;

public class SecondaryIndexExperiment {

    private final URI endpoint;

    private static AtomicLong count = new AtomicLong();

    public SecondaryIndexExperiment(URI endpoint) {
        this.endpoint = endpoint;
    }

    public void run(boolean invoke, int clients) throws Exception {
        for (int i = 0; i < clients; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {

                        String query = "tpch.primary_key(" + ThreadLocalRandom.current().nextInt() + ");";
                        if (invoke) {
                            query = "invoke function " + query;
                        }
                        try {
                            AsterixUtil.executeQuery(query);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        count.incrementAndGet();
                    }
                }
            });
            thread.start();
        }
    }

    public static void main(String[] args) throws Exception {
        boolean invoke = Boolean.valueOf(args[0]);
        int threads = Integer.valueOf(args[1]);
        URI endpoint = new URI("http://seaborgium.ics.uci.edu:19002/query/service");
        //URI endpoint = new URI("http://localhost:19002/query/service");
        AsterixUtil.init(endpoint);

        TimerTask task = new TimerTask() {
            private long last = 0;

            @Override
            public void run() {
                long count = SecondaryIndexExperiment.count.get();
                System.out.println(count - last);
                last = count;
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(task, 0, 1000);
        SecondaryIndexExperiment experiment = new SecondaryIndexExperiment(endpoint);
        experiment.run(invoke, threads);
    }

}
