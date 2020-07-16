package edu.uci.asterixdb.tpch;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.asterixdb.tpch.gen.TpchEntity;

public class TpchWorker implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Iterator<? extends TpchEntity> iterator;
    private final Socket socket;
    private final OutputStream out;
    private final AtomicLong loaded;

    public TpchWorker(Iterator<? extends TpchEntity> iterator, String url, int port, AtomicLong loaded)
            throws UnknownHostException, IOException {
        socket = new Socket(url, port);
        socket.setSendBufferSize(512 * 1024);
        out = socket.getOutputStream();
        this.iterator = iterator;
        this.loaded = loaded;
    }

    @Override
    public void run() {
        String line = null;
        try {
            while (iterator.hasNext()) {
                TpchEntity entity = iterator.next();
                line = entity.toLine();
                out.write(line.getBytes());
                out.write("\n".getBytes());
                loaded.incrementAndGet();
            }
        } catch (IOException e) {
            LOGGER.error("Fail to write line " + line, e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
