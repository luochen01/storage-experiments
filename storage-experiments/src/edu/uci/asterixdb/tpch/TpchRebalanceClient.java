package edu.uci.asterixdb.tpch;

import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.util.QueryUtil;
import edu.uci.asterixdb.tpch.gen.Distributions;
import edu.uci.asterixdb.tpch.gen.LineItemGenerator;

public class TpchRebalanceClient {

    public static final Logger LOGGER = LogManager.getLogger();

    public static final int REFRESH_PERIOD = 100;

    @Option(required = true, name = "-u")
    public String url;

    @Option(required = true, name = "-conf")
    public String conf;

    @Option(required = true, name = "-scale")
    public int scaleFactor;

    @Option(name = "-workers")
    public int numWorkers = 1;

    @Option(name = "-dss")
    public String dssPath = "";

    @Option(required = true, name = "-nodes")
    public String nodes = "";

    @Option(required = true, name = "-speed")
    public int speed;

    public ThreadPoolExecutor executor;

    public Properties properties = new Properties();

    private String[] urls;

    public static void main(String[] args) throws Exception {
        TpchRebalanceClient client = new TpchRebalanceClient(args);
        client.start();
    }

    public TpchRebalanceClient(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
    }

    public void start() throws Exception {
        if (!dssPath.isEmpty()) {
            Distributions.loadDefaults(dssPath);
        }
        QueryUtil.init(new URI(String.format("http://localhost:19002/query/service")));
        executor = new ThreadPoolExecutor(numWorkers, numWorkers, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        properties.load(new FileReader(conf));

        urls = url.split(",");

        LineItemGenerator[] lineItemGens = new LineItemGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            lineItemGens[i - 1] = new LineItemGenerator(scaleFactor, i, numWorkers, true);
        }
        process(lineItemGens);

        executor.shutdown();
    }

    private void process(LineItemGenerator[] lineItemGens) throws Exception {
        AtomicLong loaded = new AtomicLong();
        String tableName = lineItemGens[0].getName();
        LOGGER.error("Start loading {}", tableName);
        int port = Integer.valueOf(properties.getProperty(tableName));
        createFeed(tableName, port);
        List<Future<?>> result = new ArrayList<>();
        TpchWorker[] workers = new TpchWorker[lineItemGens.length];

        RateLimiter rateLimiter = new RateLimiter();

        for (int i = 0; i < lineItemGens.length; i++) {
            workers[i] = new TpchWorker(lineItemGens[i].iterator(), urls[i % urls.length], port, loaded, rateLimiter);
            Future<?> future = executor.submit(workers[i]);
            result.add(future);
        }
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            private long lastLoaded = 0;

            @Override
            public void run() {
                long count = loaded.get();
                LOGGER.error("Loaded {}/{} records of {}. Loading speed {} records/s", count,
                        lineItemGens[0].cardinality(), tableName, count - lastLoaded);
                lastLoaded = count;
            }
        }, 0, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private final int refreshAmount = speed * REFRESH_PERIOD / 1000;

            @Override
            public void run() {
                rateLimiter.add(refreshAmount);
            }
        }, 0, REFRESH_PERIOD);

        // perform rebalance
        rebalance();

        for (TpchWorker worker : workers) {
            worker.stop();
        }
        rateLimiter.stop();

        for (Future<?> future : result) {
            future.get();
        }

        finalizeFeed(tableName);

        LOGGER.error("Completed loading {} records of {}", loaded.get(), tableName);
        timer.cancel();

    }

    private void rebalance() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("dataverseName", "tpch");
        params.put("datasetName", "LineItem");
        params.put("nodes", nodes);

        QueryUtil.executePost(params, new URI(String.format("http://localhost:19002/admin/rebalanceopt")));

    }

    private void createFeed(String table, int port) throws Exception {
        String createFeedQuery = getCreateFeedString(table, port);
        QueryUtil.executeQuery("dropFeed", String.format("use tpch; drop feed %sFeed if exists;", table));
        QueryUtil.executeQuery("createFeed", createFeedQuery);
    }

    private void finalizeFeed(String table) throws Exception {
        String finalizeFeedQuery = getFinalizeFeedString(table);
        QueryUtil.executeQuery("finalizeFeed", finalizeFeedQuery);
    }

    private String getCreateFeedString(String table, int port) {
        String createFeed = String.format(
                "use tpch;\n" + " create feed %sFeed with {\n" + "    \"adapter-name\" : \"socket\",\n"
                        + "    \"sockets\" : \"%s\",\n" + "    \"address-type\" : \"nc\",\n"
                        + "    \"type-name\" : \"%s\",\n" + "    \"format\" : \"delimited-text\",\n"
                        + "    \"delimiter\": \"|\",\n" + "    \"insert-feed\" : \"false\"\n" + "};\n",
                table, getAddressString(port), table + "Type");

        String startFeed =
                String.format("connect feed %sFeed to dataset %s;   \n" + "start feed %sFeed;", table, table, table);
        return createFeed + startFeed;
    }

    private String getAddressString(int port) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= Math.min(numWorkers, urls.length); i++) {
            sb.append(String.format("%d:%d", i, port));
            if (i < numWorkers) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private String getFinalizeFeedString(String table) {
        return String.format("use tpch;stop feed %sFeed; drop feed %sFeed;", table, table);
    }

}
