package edu.uci.asterixdb.storage.experiments.feed.gen;

import edu.uci.asterixdb.storage.experiments.util.ZipfianGenerator;

public class ZipfIdTimeGenerator extends IdGenerator {

    private static final long MIN_ITEM_COUNT = 2;

    private final ZipfianGenerator gen;

    public ZipfIdTimeGenerator(long startRange, double updateRatio, boolean randomize) {
        super(startRange, updateRatio, randomize);
        gen = new ZipfianGenerator(MIN_ITEM_COUNT);
    }

    public ZipfIdTimeGenerator(long startRange, double updateRatio, boolean randomize, double zipfConst) {
        super(startRange, updateRatio, randomize);
        gen = new ZipfianGenerator(0, MIN_ITEM_COUNT - 1, zipfConst);
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

    public static void main(String[] args) {
        // each page can hold 128KB / 0.5K = 256 tweets
        // we have 1.5GB buffer cache = 12288 pages
        // in total, we get 3145728 tweets can be cached
        int cacheTweets = 3145728 * 2;
        int period = 10000000;
        long totalTweets = 100 * 1000000;
        double updateRatio = 0.1;

        simulate(new ZipfIdTimeGenerator(0, updateRatio, false, 0.99), cacheTweets, totalTweets, period);
        simulate(new ZipfIdTimeGenerator(0, updateRatio, false, 0.9), cacheTweets, totalTweets, period);
        simulate(new ZipfIdTimeGenerator(0, updateRatio, false, 0.5), cacheTweets, totalTweets, period);
        simulate(new UniformIdGenerator(0, updateRatio, false), cacheTweets, totalTweets, period);
    }

    private static void simulate(IdGenerator gen, int cacheTweets, long totalTweets, int period) {
        System.out.println("Simulate: " + gen);
        long maxId = 0L;
        int diskIO = 0;
        int lastDiskIO = 0;
        for (long i = 0; i < totalTweets; i++) {
            long id = gen.next();
            if (id < maxId - cacheTweets) {
                diskIO++;
            } else {
                maxId = Math.max(maxId, id);
            }

            if (i % period == 0) {
                System.out.println(i + " " + diskIO + " " + (diskIO - lastDiskIO));
                lastDiskIO = diskIO;
            }
        }

    }

}
