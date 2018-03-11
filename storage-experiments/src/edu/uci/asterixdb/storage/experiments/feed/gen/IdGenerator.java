package edu.uci.asterixdb.storage.experiments.feed.gen;

import java.util.Random;

import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.UpdateDistribution;
import edu.uci.asterixdb.storage.experiments.util.ScrambledZipfianGenerator;

public abstract class IdGenerator {

    protected final long startRange;
    protected final double updateRatio;
    protected final boolean randomize;

    protected long counter = 0;
    protected final Random random = new Random(23);

    public IdGenerator(long startRange, double updateRatio, boolean randomize) {
        this.startRange = startRange;
        this.updateRatio = updateRatio;
        this.randomize = randomize;
    }

    public static IdGenerator create(UpdateDistribution dist, long startRange, double updateRatio, boolean randomize) {
        switch (dist) {
            case UNIFORM:
                return new UniformIdGenerator(startRange, updateRatio, randomize);
            case ZIPF_ITEM:
                return new ZipfIdItemGenerator(startRange, updateRatio, randomize);
            case ZIPF_TIME:
                return new ZipfIdTimeGenerator(startRange, updateRatio, randomize);
            default:
                throw new IllegalArgumentException("Unknown distribution " + dist);
        }
    }

    public final long next() {
        long id = 0L;
        if (random.nextDouble() < updateRatio) {
            id = generateUpdate(counter + 1) + startRange;
        } else {
            id = (counter++);
        }
        return randomize ? randomize(id) : id;
    }

    private long randomize(long id) {
        int higher = ScrambledZipfianGenerator.fnvhash32((int) id);
        return (higher << 32) | id;
    }

    protected abstract long generateUpdate(long itemCount);

    public static void main(String[] agrs) {
        int count = 100;
        double updateRatio = 0.5;
        IdGenerator gen = IdGenerator.create(UpdateDistribution.UNIFORM, 0, updateRatio, false);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.UNIFORM, 0, updateRatio, true);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.ZIPF_ITEM, 0, updateRatio, false);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.ZIPF_ITEM, 0, updateRatio, true);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.ZIPF_TIME, 0, updateRatio, false);
        test(gen, count);

        gen = IdGenerator.create(UpdateDistribution.ZIPF_TIME, 0, updateRatio, true);
        test(gen, count);
    }

    private static void test(IdGenerator gen, int count) {
        System.out.println("Test " + gen);
        for (int i = 0; i < count; i++) {
            System.out.println(gen.next());
        }
    }

}
