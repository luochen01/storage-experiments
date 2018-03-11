package edu.uci.asterixdb.storage.experiments.feed.gen;

import edu.uci.asterixdb.storage.experiments.util.ZipfianGenerator;

public class ZipfIdTimeGenerator extends IdGenerator {

    private static final long MIN_ITEM_COUNT = 2;

    private final ZipfianGenerator gen = new ZipfianGenerator(MIN_ITEM_COUNT);

    public ZipfIdTimeGenerator(long startRange, double updateRatio, boolean randomize) {
        super(startRange, updateRatio, randomize);
    }

    @Override
    protected long generateUpdate(long itemCount) {
        itemCount = Math.max(itemCount, MIN_ITEM_COUNT);
        long value = gen.nextLong(itemCount);
        if (value > itemCount) {
            throw new IllegalStateException("Too large value " + value + " item count " + itemCount);
        }
        return itemCount - value;
    }

}
