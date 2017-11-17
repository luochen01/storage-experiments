package edu.uci.asterixdb.storage.experiments.feed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.sun.management.OperatingSystemMXBean;

public class FeedReporter extends TimerTask {

    protected final int period;

    protected final Timer timer;

    protected long counter = 0;

    protected FileFeedDriver driver;

    protected List<FeedSocketAdapterClient> clients;

    protected List<FeedStat> prevStats;

    protected FeedStat prevAllStat = new FeedStat();

    private final OperatingSystemMXBean bean;

    protected NumberFormat cpuFormat = new DecimalFormat("#0.000");

    protected BufferedWriter logWriter;

    public FeedReporter(FileFeedDriver driver, List<FeedSocketAdapterClient> clients, int period, String logPath)
            throws IOException {
        this.period = period;
        this.timer = new Timer();
        this.driver = driver;

        this.clients = clients;
        this.prevStats = new ArrayList<>();
        for (int i = 0; i < clients.size(); i++) {
            this.prevStats.add(new FeedStat());
        }
        bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        File logFile = new File(logPath);
        if (logFile.getParentFile() != null) {
            if (!logFile.getParentFile().exists()) {
                logFile.mkdirs();
            }
        }
        logWriter = new BufferedWriter(new FileWriter(logFile));

        writeLine("port,counter,records,bytes,total_records,total_bytes,cpu\n");
    }

    @Override
    public void run() {
        StringBuilder sb = new StringBuilder();
        if (clients.size() > 1) {
            for (int i = 0; i < clients.size(); i++) {
                FeedSocketAdapterClient client = clients.get(i);
                FeedStat clientStat = client.stat;
                FeedStat prevStat = prevStats.get(i);
                sb.append(getLine(clientStat, prevStat, String.valueOf(client.port), "-"));
                prevStat.update(clientStat);
            }
        }

        FeedStat clientStat = FeedStat.sum(clients);
        sb.append(getLine(clientStat, prevAllStat, "-", "-"));
        prevAllStat.update(clientStat);

        try {
            writeLine(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        counter += (period);

    }

    public void start() {
        timer.schedule(this, period * 1000, period * 1000);
    }

    protected String getLine(FeedStat stat, FeedStat prevStat, String port, String cpu) {
        long records = (stat.totalRecords - prevStat.totalRecords);
        long bytes = (stat.totalBytes - prevStat.totalBytes);
        long totalRecords = stat.totalRecords;
        long totalBytes = stat.totalBytes;
        return port + "," + counter + "," + records + "," + bytes + "," + totalRecords + "," + totalBytes + "," + cpu
                + "\n";
    }

    protected void writeLine(String line) throws IOException {
        System.out.print(line);
        logWriter.write(line);
    }

    public void flush() throws IOException {
        logWriter.flush();
    }

    public void close() throws IOException {
        timer.cancel();
        logWriter.close();
    }

}
