package edu.uci.asterixdb.storage.experiments.feed.gen;

import java.util.Random;

import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.UpdateDistribution;
import edu.uci.asterixdb.storage.experiments.util.ScrambledZipfianGenerator;
import edu.uci.asterixdb.storage.experiments.util.ZipfianGenerator;

public abstract class IdGenerator {

    protected final long startRange;
    protected final double updateRatio;
    protected final boolean randomize;

    protected long counter;
    protected final Random random = new Random(23);

    private boolean isNewTweet;

    public IdGenerator(long startRange, double updateRatio, boolean randomize) {
        this.startRange = startRange;
        this.updateRatio = updateRatio;
        this.randomize = randomize;
        this.counter = startRange;
    }

    public static IdGenerator create(UpdateDistribution dist, double theta, long startRange, double updateRatio,
            boolean randomize) {
        switch (dist) {
            case UNIFORM:
                return new UniformIdGenerator(startRange, updateRatio, randomize);
            case ZIPF:
                return new ZipfIdTimeGenerator(startRange, updateRatio, randomize, theta);
            default:
                throw new IllegalArgumentException("Unknown distribution " + dist);
        }
    }

    public final long next() {
        long id = 0L;
        if (random.nextDouble() < updateRatio) {
            id = generateUpdate(counter);
            isNewTweet = false;
        } else {
            id = (counter++);
            isNewTweet = true;
        }
        return randomize ? randomize(id) : id;
    }

    public boolean isNewTweet() {
        return isNewTweet;
    }

    private long randomize(long id) {
        short hash = (short) ScrambledZipfianGenerator.fnvhash64(id);
        // we use 16 high order bits for hashing
        return ((long) hash << 48) | id;
    }

    protected abstract long generateUpdate(long itemCount);

    public static void main(String[] agrs) {
        int count = 100;
        double updateRatio = 0.5;
        IdGenerator gen = IdGenerator.create(UpdateDistribution.UNIFORM, ZipfianGenerator.ZIPFIAN_CONSTANT, 0,
                updateRatio, false);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.UNIFORM, ZipfianGenerator.ZIPFIAN_CONSTANT, 0, updateRatio, true);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.ZIPF, ZipfianGenerator.ZIPFIAN_CONSTANT, 0, updateRatio, false);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.ZIPF, ZipfianGenerator.ZIPFIAN_CONSTANT, 0, updateRatio, true);
        test(gen, count);
    }

    private static void test(IdGenerator gen, int count) {
        System.out.println("Test " + gen);
        for (int i = 0; i < count; i++) {
            System.out.println(gen.next());
        }
    }

}
