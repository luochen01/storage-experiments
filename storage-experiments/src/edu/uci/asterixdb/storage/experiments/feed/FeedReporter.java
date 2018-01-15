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

    protected FileFeedDriver driver;

    protected FeedSocketAdapterClient client;

    protected FeedStat prevStat = new FeedStat();

    protected BufferedWriter logWriter;

    public FeedReporter(FileFeedDriver driver, FeedSocketAdapterClient client, int period, String logPath)
            throws IOException {
        this.period = period;
        this.timer = new Timer();
        this.driver = driver;

        this.client = client;
        this.prevStat = new FeedStat();

        File logFile = new File(logPath);
        if (logFile.getParentFile() != null) {
            if (!logFile.getParentFile().exists()) {
                logFile.mkdirs();
            }
        }
        logWriter = new BufferedWriter(new FileWriter(logFile));

        writeLine("counter,records,bytes,total_records,total_bytes\n");
    }

    @Override
    public void run() {
        StringBuilder sb = new StringBuilder();
        FeedStat clientStat = client.stat;
        sb.append(getLine(clientStat, prevStat));
        prevStat.update(clientStat);

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

    protected String getLine(FeedStat stat, FeedStat prevStat) {
        long records = (stat.totalRecords - prevStat.totalRecords);
        long bytes = (stat.totalBytes - prevStat.totalBytes);
        long totalRecords = stat.totalRecords;
        long totalBytes = stat.totalBytes;
        return counter + "," + records + "," + bytes + "," + totalRecords + "," + totalBytes + "\n";
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
