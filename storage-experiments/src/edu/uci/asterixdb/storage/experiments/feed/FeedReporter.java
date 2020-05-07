package edu.uci.asterixdb.storage.experiments.feed;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class FeedReporter extends TimerTask {

    protected final int period;

    protected final Timer timer;

    protected long counter = 0;

    protected FeedSocketAdapterClient[] clients;

    protected FeedStat prevStat = new FeedStat();

    protected BufferedWriter logWriter;

    public FeedReporter(FeedSocketAdapterClient[] clients, int period, String logPath) throws IOException {
        this.period = period;
        this.timer = new Timer();

        this.clients = clients;
        this.prevStat = new FeedStat();

        File logFile = new File(logPath);
        if (logFile.getParentFile() != null) {
            if (!logFile.getParentFile().exists()) {
                logFile.getParentFile().mkdirs();
            }
        }
        logWriter = new BufferedWriter(new FileWriter(logFile));
    }

    @Override
    public void run() {
        StringBuilder sb = new StringBuilder();
        FeedStat totalStat = FeedStat.sum(clients);
        sb.append(getLine(totalStat, prevStat));
        prevStat.update(totalStat);
        try {
            writeLine(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        counter += (period);
    }

    public void start() throws IOException {
        writeLine("counter,records,bytes,total_records,total_bytes,insert_records,update_records");
        timer.schedule(this, period * 1000, period * 1000);
    }

    protected String getLine(FeedStat stat, FeedStat prevStat) {
        long records = (stat.totalRecords - prevStat.totalRecords);
        long bytes = (stat.totalBytes - prevStat.totalBytes);
        long totalRecords = stat.totalRecords;
        long totalBytes = stat.totalBytes;
        return counter + "," + records + "," + bytes + "," + totalRecords + "," + totalBytes + "," + stat.insertRecords
                + "," + stat.updateRecords;
    }

    public void writeLine(String line) throws IOException {
        System.out.println(line);
        logWriter.write(line);
        logWriter.write('\n');
        logWriter.flush();
    }

    public void flush() throws IOException {
        logWriter.flush();
    }

    public void close() throws IOException {
        timer.cancel();
        logWriter.close();
    }

}
