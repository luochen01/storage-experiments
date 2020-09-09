package edu.uci.asterixdb.tpch;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
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

    public ThreadPoolExecutor executor;

    public Properties properties = new Properties();

    private String[] urls;

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

        executor = new ThreadPoolExecutor(numWorkers, numWorkers, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        properties.load(new FileReader(conf));

        urls = url.split(",");

        NationGenerator nationGen = new NationGenerator();
        process(nationGen.getName(), nationGen);

        RegionGenerator regionGen = new RegionGenerator();
        process(regionGen.getName(), regionGen);

        LineItemGenerator[] lineItemGens = new LineItemGenerator[numWorkers];
        for (int i = 1; i <= numWorkers; i++) {
            lineItemGens[i - 1] = new LineItemGenerator(scaleFactor, i, numWorkers);
        }
        process(lineItemGens[0].getName(), lineItemGens);

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

        executor.shutdown();
    }

    private void process(String table, TpchGenerator... gens) throws Exception {
        AtomicLong loaded = new AtomicLong();
        LOGGER.error("Start loading {}", table);
        int port = Integer.valueOf(properties.getProperty(table));
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
        LOGGER.error("Completed loading {} records of {}", loaded.get(), table);
        timer.cancel();
    }

}
