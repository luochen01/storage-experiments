package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class FeedSocketAdapterClient {
    private OutputStream out = null;

    protected String adapterUrl;
    protected int port;
    protected int waitMillSecond;
    protected int batchSize;
    protected Socket socket;

    public final FeedStat stat = new FeedStat();

    public FeedSocketAdapterClient(String adapterUrl, int port) {
        this.adapterUrl = adapterUrl;
        this.port = port;
    }

    public void initialize() throws IOException {
        socket = new Socket(adapterUrl, port);
        // 512K
        socket.setSendBufferSize(512 * 1024);
        out = socket.getOutputStream();
    }

    @Override
    public void finalize() {
        try {
            out.close();
            socket.close();
        } catch (IOException e) {
            System.err.println("Problem in closing socket against host " + adapterUrl + " on the port " + port);
            e.printStackTrace();
        }
    }

    public void ingest(String record, boolean newRecord) throws IOException {
        byte[] b = record.getBytes();
        out.write(b);
        //out.flush();
        stat.totalRecords++;
        stat.totalBytes += b.length;
        if (newRecord) {
            stat.insertRecords++;
        } else {
            stat.updateRecords++;
        }
    }

    public int getPort() {
        return port;
    }

}
