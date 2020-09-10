package edu.uci.asterixdb.tpch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.asterixdb.storage.experiments.util.QueryResult;
import edu.uci.asterixdb.storage.experiments.util.QueryUtil;

public class TpchQueryClient {

    public static final Logger LOGGER = LogManager.getLogger();

    @Option(required = true, name = "-u")
    public String url;

    @Option(required = true, name = "-path")
    public String path;

    @Option(required = true, name = "-output")
    public String output;

    @Option(name = "-result")
    public String result;

    @Option(name = "-query")
    public String query = "";

    private BufferedWriter outputWriter;
    private BufferedWriter resultWriter;

    public static void main(String[] args) throws Exception {
        TpchQueryClient client = new TpchQueryClient(args);
        client.run();
    }

    public TpchQueryClient(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
        QueryUtil.init(new URI(String.format("http://%s:19002/query/service", url)));

        if (output != null) {
            outputWriter = new BufferedWriter(new FileWriter(new File(output)));
        }
        if (result != null) {
            resultWriter = new BufferedWriter(new FileWriter(new File(result)));
        }
    }

    public void run() throws Exception {
        if (query.isEmpty()) {
            String[] queries = query.split(",");
            for (String q : queries) {
                run(Integer.valueOf(q));
            }
        } else {
            for (int i = 1; i <= 22; i++) {
                run(i);
            }
        }

        if (outputWriter != null) {
            outputWriter.close();
        }
        if (resultWriter != null) {
            resultWriter.close();
        }
    }

    private void run(int queryId) {
        try {
            String queryPath = path + "/q" + queryId + ".sqlpp";
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new FileReader(new File(queryPath)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            reader.close();

            QueryResult result = QueryUtil.executeQuery("q" + queryId, sb.toString());
            append(outputWriter, "query\t" + result.parameter + "\t" + result.time);
            append(resultWriter, result.result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void append(BufferedWriter writer, Object content) throws IOException {
        if (writer != null && content != null) {
            writer.append(content.toString());
            writer.append("\n");
            writer.flush();
        }
    }

}
