package edu.uci.asterixdb.storage.experiments.feed;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import edu.uci.asterixdb.storage.experiments.feed.gen.IRecordGenerator;

public class FeedSocketAdapterClient {
    private OutputStream out = null;

    protected String adapterUrl;
    protected int port;
    protected int waitMillSecond;
    protected int batchSize;
    protected Socket socket;

    public final FeedStat stat = new FeedStat();

    public final IRecordGenerator recordGen;

    public FeedSocketAdapterClient(String adapterUrl, int port, IRecordGenerator recordGen) {
        this.adapterUrl = adapterUrl;
        this.port = port;
        this.recordGen = recordGen;
    }

    public void initialize() throws IOException {
        socket = new Socket(adapterUrl, port);
        // 512K
        socket.setSendBufferSize(512 * 1024);
        out = socket.getOutputStream();
    }

    public void reconnect() throws IOException {
        close();
        initialize();
    }

    public void close() {
        if (socket != null) {
            try {
                out.close();
                socket.close();
                out = null;
                socket = null;
            } catch (IOException e) {
                System.err.println("Problem in closing socket against host " + adapterUrl + " on the port " + port);
                e.printStackTrace();
            }
        }
    }

    public void ingest(long id, boolean newRecord) throws IOException {
        //System.out.println(record);
        String record = recordGen.getRecord(id);
        byte[] b = record.getBytes();
        out.write(b);
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
