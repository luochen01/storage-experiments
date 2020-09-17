package edu.uci.asterixdb.tpch;

import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
import edu.uci.asterixdb.tpch.gen.CustomerGenerator;
import edu.uci.asterixdb.tpch.gen.Distributions;
import edu.uci.asterixdb.tpch.gen.LineItemGenerator;
import edu.uci.asterixdb.tpch.gen.NationGenerator;
import edu.uci.asterixdb.tpch.gen.OrderGenerator;
import edu.uci.asterixdb.tpch.gen.PartGenerator;
import edu.uci.asterixdb.tpch.gen.PartsuppGenerator;
import edu.uci.asterixdb.tpch.gen.RegionGenerator;
import edu.uci.asterixdb.tpch.gen.SupplierGenerator;
import edu.uci.asterixdb.tpch.gen.TpchGenerator;

public class TpchClient {

    public static final Logger LOGGER = LogManager.getLogger();

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

    @Option(name = "-table")
    public String table;

    public ThreadPoolExecutor executor;

    public Properties properties = new Properties();

    private String[] urls;

    private Set<String> tables = new HashSet<>();

    public static void main(String[] args) throws Exception {
        TpchClient client = new TpchClient(args);
        client.start();
    }

    public TpchClient(String[] args) throws Exception {
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
        tables.addAll(Arrays.asList(table.split(",")));
        urls = url.split(",");

        NationGenerator nationGen = new NationGenerator();
        process(nationGen.getName(), nationGen);

        RegionGenerator regionGen = new RegionGenerator();
        process(regionGen.getName(), regionGen);

        SupplierGenerator[] supplierGen = new SupplierGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            supplierGen[i - 1] = new SupplierGenerator(scaleFactor, i, numWorkers);
        }
        process(supplierGen[0].getName(), supplierGen);

        PartGenerator[] partGens = new PartGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            partGens[i - 1] = new PartGenerator(scaleFactor, i, numWorkers);
        }
        process(partGens[0].getName(), partGens);

        PartsuppGenerator[] partSupplierGens = new PartsuppGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            partSupplierGens[i - 1] = new PartsuppGenerator(scaleFactor, i, numWorkers);
        }
        process(partSupplierGens[0].getName(), partSupplierGens);

        CustomerGenerator[] customerGens = new CustomerGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            customerGens[i - 1] = new CustomerGenerator(scaleFactor, i, numWorkers);
        }
        process(customerGens[0].getName(), customerGens);

        OrderGenerator[] orderGens = new OrderGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            orderGens[i - 1] = new OrderGenerator(scaleFactor, i, numWorkers);
        }
        process(orderGens[0].getName(), orderGens);

        LineItemGenerator[] lineItemGens = new LineItemGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            lineItemGens[i - 1] = new LineItemGenerator(scaleFactor, i, numWorkers);
        }
        process(lineItemGens[0].getName(), lineItemGens);

        executor.shutdown();
    }

    private void process(String table, TpchGenerator... gens) throws Exception {
        if (!tables.isEmpty() && !tables.contains(table)) {
            LOGGER.error("Skipped {}", table);
            return;
        }
        AtomicLong loaded = new AtomicLong();
        LOGGER.error("Start loading {}", table);
        int port = Integer.valueOf(properties.getProperty(table));
        createFeed(table, port);
        List<Future<?>> result = new ArrayList<>();
        for (int i = 0; i < gens.length; i++) {
            Future<?> future = executor.submit(new TpchWorker(gens[i].iterator(), urls[i % urls.length], port, loaded));
            result.add(future);
        }
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {

            private long lastLoaded = 0;

            @Override
            public void run() {
                long count = loaded.get();
                LOGGER.error("Loaded {}/{} records of {}. Loading speed {} records/s", count, gens[0].cardinality(),
                        table, count - lastLoaded);
                lastLoaded = count;
            }
        }, 0, 1000);

        for (Future<?> future : result) {
            future.get();
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        finalizeFeed(table);

        LOGGER.error("Completed loading {} records of {}", loaded.get(), table);
        timer.cancel();

    }

    private void createFeed(String table, int port) throws Exception {
        String createFeedQuery = getCreateFeedString(table, port);
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
                        + "    \"delimiter\": \"|\",\n" + "    \"insert-feed\" : \"true\"\n" + "};\n",
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
