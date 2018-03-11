package edu.uci.asterixdb.storage.experiments.feed.gen;

public class UniformIdGenerator extends IdGenerator {

    public UniformIdGenerator(long startRange, double updateRatio, boolean randomize) {
        super(startRange, updateRatio, randomize);
    }

    @Override
    protected long generateUpdate(long itemCount) {
        return Math.abs(random.nextLong()) % itemCount;
    }

}
