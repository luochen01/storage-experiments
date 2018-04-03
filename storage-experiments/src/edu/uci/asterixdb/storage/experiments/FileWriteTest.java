package edu.uci.asterixdb.storage.experiments;

import java.io.RandomAccessFile;
import java.util.concurrent.Semaphore;

public class FileWriteTest {

    public static void main(String args[]) throws Exception {

        String file = "test.file";

        Semaphore writeFinished = new Semaphore(0);
        Semaphore readerCreated = new Semaphore(0);
        Semaphore writerCreated = new Semaphore(0);
        Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    RandomAccessFile writer = new RandomAccessFile(file, "rw");
                    writerCreated.release();

                    readerCreated.acquire();
                    writer.write(1);
                    writeFinished.release();

                    Thread.sleep(100);
                    writer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    writerCreated.acquire();
                    RandomAccessFile reader = new RandomAccessFile(file, "r");
                    readerCreated.release();

                    writeFinished.acquire();
                    byte value = reader.readByte();
                    System.out.println(value);

                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        t1.start();
        t2.start();

    }

}
