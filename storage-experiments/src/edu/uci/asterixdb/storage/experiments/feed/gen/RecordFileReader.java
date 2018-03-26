package edu.uci.asterixdb.storage.experiments.feed.gen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class RecordFileReader implements IRecordGenerator {

    private final BufferedReader reader;

    private boolean ended = false;

    public RecordFileReader(String path) throws IOException {
        reader = new BufferedReader(new FileReader(path));
    }

    @Override
    public String getNext() throws IOException {
        if (ended) {
            return null;
        }
        String line = reader.readLine();
        if (line == null) {
            reader.close();
            ended = true;
        }
        return line;
    }

    @Override
    public boolean isNewRecord() {
        return true;
    }

}
