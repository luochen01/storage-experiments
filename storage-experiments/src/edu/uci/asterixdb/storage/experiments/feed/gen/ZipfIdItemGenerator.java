package edu.uci.asterixdb.storage.experiments.feed.gen;

import edu.uci.asterixdb.storage.experiments.util.ScrambledZipfianGenerator;

public class ZipfIdItemGenerator extends IdGenerator {
    private static final long MIN_ITEM_COUNT = 2;

    private final ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(MIN_ITEM_COUNT);

    public ZipfIdItemGenerator(long startRange, double updateRatio, boolean randomize) {
        super(startRange, updateRatio, randomize);
    }

    @Override
    protected long generateUpdate(long itemCount) {
        return gen.nextValue(Math.max(itemCount, MIN_ITEM_COUNT));
    }

}
