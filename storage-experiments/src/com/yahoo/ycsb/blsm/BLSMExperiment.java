package com.yahoo.ycsb.blsm;

import java.io.File;
import java.io.PrintWriter;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.yahoo.mapkeeper.MapKeeper;
import com.yahoo.ycsb.Client;

public class BLSMExperiment {

    @Option(name = "-total", usage = "total time")
    public int totalTime = 7200;

    @Option(name = "-load", usage = "load")
    public boolean load = false;

    @Option(name = "-threads", usage = "threads")
    public int threads = 1;

    @Option(name = "-url", usage = "url")
    public String url = "nobelium.ics.uci.edu";

    @Option(required = true, name = "-output", usage = "url")
    public String output = "";

    @Option(name = "-limit", usage = "speed limit")
    public int limitSpeed = -1;

    @Option(name = "-shutdown", usage = "speed limit")
    public boolean shutdown = false;

    public BLSMExperiment(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);

        BLSMAdapter.URL = url;

        if (shutdown) {
            shutdown();
            return;
        }
        initializeTimer();
        if (load) {
            doLoad();
        } else {
            doRun();
        }
    }

    private void shutdown() throws TException {
        TSocket socket = new TSocket(url, 9090);
        TTransport trans = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(trans);
        MapKeeper.Client client = new MapKeeper.Client(protocol);
        trans.open();
        client.shutdown();
        trans.close();
    }

    private void initializeTimer() throws Exception {
        PrintWriter writer = new PrintWriter(new File(output));

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            long prevRecords = 0;
            int counter = 0;

            @Override
            public void run() {

                long totalRecords = BLSMAdapter.operationCount.get();
                long records = totalRecords - prevRecords;

                String line = counter + "\t" + records + "\t" + totalRecords;
                System.out.println(line);
                writer.println(line);
                //writer.flush();
                prevRecords = totalRecords;
                counter++;
            }
        }, 0, 1000);
    }

    private void doLoad() throws Exception {
        // load the dataset
        createDB();

        String loadStr = "-load";
        String workloadStr = "-P workloada";
        String dbStr = "-db com.yahoo.ycsb.blsm.BLSMAdapter";
        String threadStr = "-threads " + threads;
        String arg = workloadStr + " " + dbStr + " " + loadStr + " " + threadStr;
        System.out.println("Arg " + arg);
        Client.main(arg.split(" "));
        Runtime.getRuntime().halt(0);
    }

    private void doRun() throws Exception {
        String workloadStr = "-P workloada";
        String dbStr = " -db com.yahoo.ycsb.blsm.BLSMAdapter";
        String targetStr = "";
        if (limitSpeed > 0) {
            targetStr = " -target " + limitSpeed;
        }
        String threadStr = " -threads " + threads;
        String maxTimeStr = " -p maxexecutiontime=" + totalTime;
        String arg = workloadStr + dbStr + targetStr + maxTimeStr + threadStr;
        System.out.println("Arg " + arg);
        Client.main(arg.trim().split(" "));
        Runtime.getRuntime().halt(0);
    }

    private void createDB() throws TException {
        TSocket socket = new TSocket(url, 9090);
        TTransport trans = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(trans);
        MapKeeper.Client client = new MapKeeper.Client(protocol);
        trans.open();

        client.dropMap(BLSMAdapter.DB_NAME);
        client.addMap(BLSMAdapter.DB_NAME);

        trans.close();
    }

    public static void main(String[] args) throws Exception {
        new BLSMExperiment(args);
    }

}
