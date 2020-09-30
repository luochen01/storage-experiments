package edu.uci.asterixdb.tpch;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.asterixdb.tpch.gen.TpchEntity;

public class TpchWorker implements Runnable {

    private static final int BATCH_SIZE = 100;

    private static final Logger LOGGER = LogManager.getLogger();

    private final Iterator<? extends TpchEntity> iterator;
    private final Socket socket;
    private final OutputStream out;
    private final AtomicLong loaded;
    private final IRateLimiter rateLimiter;
    private final AtomicBoolean stopped = new AtomicBoolean();

    public TpchWorker(Iterator<? extends TpchEntity> iterator, String url, int port, AtomicLong loaded,
            IRateLimiter rateLimiter) throws UnknownHostException, IOException {
        socket = new Socket(url, port);
        socket.setSendBufferSize(512 * 1024);
        out = socket.getOutputStream();
        this.iterator = iterator;
        this.loaded = loaded;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void run() {
        String line = null;
        try {
            while (!stopped.get() && iterator.hasNext()) {
                int batch = rateLimiter.get(BATCH_SIZE);
                while (batch > 0 && iterator.hasNext() && !stopped.get()) {
                    TpchEntity entity = iterator.next();
                    line = entity.toLine();
                    out.write(line.getBytes());
                    out.write("\n".getBytes());
                    loaded.incrementAndGet();
                    batch--;
                }
            }
        } catch (IOException e) {
            LOGGER.error("Fail to write line " + line, e);
        } finally {
            try {
                out.flush();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        stopped.set(true);
    }

}
