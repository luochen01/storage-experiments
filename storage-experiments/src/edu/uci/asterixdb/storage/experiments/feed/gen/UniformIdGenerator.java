package edu.uci.asterixdb.storage.experiments.feed.gen;

import java.util.HashSet;
import java.util.Set;

public class UniformIdGenerator extends IdGenerator {

    public UniformIdGenerator(long startRange, double updateRatio, boolean randomize) {
        super(startRange, updateRatio, randomize);
    }

    @Override
    protected long generateUpdate(long itemCount) {
        return Math.abs(random.nextLong()) % itemCount;
    }

    public static void main(String[] args) {
        UniformIdGenerator gen = new UniformIdGenerator(0, 0.5, true);
        Set<Long> set = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            long value = gen.next();
            System.out.println(value);
            set.add(value);
        }
        System.out.println(set.size());
    }
}
