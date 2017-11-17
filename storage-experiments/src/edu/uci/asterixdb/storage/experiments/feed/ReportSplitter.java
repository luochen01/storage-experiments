package edu.uci.asterixdb.storage.experiments.feed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class ReportSplitter {

    @Argument(index = 0)
    private String input;

    @Argument(index = 1)
    private String output;

    public static void main(String[] args) throws Exception {
        new ReportSplitter().doMain(args);

    }

    public void doMain(String[] args) throws CmdLineException, IOException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        BufferedReader reader = new BufferedReader(new FileReader(input));

        String header = reader.readLine();

        String line = null;
        Map<String, BufferedWriter> map = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            String port = parts[0];

            BufferedWriter writer = map.get(port);
            if (writer == null) {
                if (port.equals("-")) {
                    writer = new BufferedWriter(new FileWriter(output + "." + "all"));
                    System.out.println("created file " + output + "." + "all");
                } else {
                    writer = new BufferedWriter(new FileWriter(output + "." + port));
                    System.out.println("created file " + output + "." + port);
                }
                writer.write(header);
                writer.write("\n");
                map.put(port, writer);
            }
            writer.write(line);
            writer.write("\n");
        }

        reader.close();
        for (BufferedWriter writer : map.values()) {
            writer.flush();
            writer.close();
        }
    }
}
