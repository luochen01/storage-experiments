package edu.uci.asterixdb.storage.experiments.lsm;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class StaticFilterExperiment {

    public static final String dataset = "ds_tweet";

    @Option(name = "-dv", aliases = "--dataverse", usage = "the dataverse name", required = true)
    public String dataverseName;

    @Option(name = "-cdv", aliases = "--cleandataverse", usage = "the dataverse name of clean cache")
    public String cleanCacheDataverse;

    @Option(name = "-min", aliases = "--min", usage = "min value of created_at", required = true)
    public String minValue;

    @Option(name = "-max", aliases = "--max", usage = "max value of created_at", required = false)
    public String maxValue;

    @Option(name = "-n", aliases = "--num", usage = "the number of queries", required = true)
    public int numQueries;

    @Option(name = "-o", aliases = "--output", usage = "output path", required = true)
    public String outputPath;

    @Option(name = "-ra", aliases = "--readahead", usage = "readAheadMemorySize(KB)")
    public long readAheadKB = 4 * 1024;

    private final Random rand = new Random(System.currentTimeMillis());

    public StaticFilterExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        System.out.println("Dataverse: " + dataverseName);
        System.out.println("Num queries: " + numQueries);
        System.out.println("Output path: " + outputPath);
        System.out.println("Clean cache dataverse: " + cleanCacheDataverse);
        System.out.println("Read ahead memory size (KB): " + readAheadKB);
        System.out.println("min value: " + minValue);

    }

    public void run() throws Exception {
        PrintWriter writer = new PrintWriter(new File(outputPath));
        writer.println("seq\ttime\tresult");
        for (int i = 1; i < numQueries; i++) {
            if (cleanCacheDataverse != null) {
                String query = LSMExperimentUtil.generateCountQuery(cleanCacheDataverse, dataset);
                QueryUtil.executeQuery("clean", query);
            }
            String query = generateFilterQuery(minValue);
            QueryResult result = QueryUtil.executeQuery("default", query);
            writer.println(i + "\t" + result.time + "\t" + result.result);
        }
        writer.flush();
        writer.close();
    }

    private String generateFilterQuery(String minValue) {
        String batch = String.format("set `compiler.readaheadmemory` '%dKB';", readAheadKB);
        String query = maxValue != null ? String.format(
                "select count(*) from %s.%s where created_at>=datetime(\"%s\") AND created_at <= datetime(\"%s\");",
                dataverseName, dataset, minValue, maxValue)
                : String.format("select count(*) from %s.%s where created_at>=datetime(\"%s\");", dataverseName,
                        dataset, minValue);
        return batch + query;
    }

    public static void main(String[] args) throws Exception {
        QueryUtil.init(new URI("http://localhost:19002/query/service"));
        StaticFilterExperiment expr = new StaticFilterExperiment(args);
        expr.run();
    }

}
