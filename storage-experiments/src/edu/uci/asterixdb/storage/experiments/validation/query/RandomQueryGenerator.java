package edu.uci.asterixdb.storage.experiments.validation.query;

import java.util.Random;

import edu.uci.asterixdb.storage.experiments.util.QueryGenerator;

public class RandomQueryGenerator {
    private final String dataverse;

    private final String dataset;

    private final int minRange;

    private final int maxRange;

    private final int queryRange;

    private Random random = new Random(17);

    private final boolean skipPkIndex;

    public RandomQueryGenerator(String dataverse, String dataset, int minRange, int maxRange, int queryRange,
            boolean skipPkIndex) {
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.minRange = minRange;
        this.maxRange = maxRange;
        this.queryRange = queryRange;
        this.skipPkIndex = skipPkIndex;
    }

    public String next() {
        int nextMin = random.nextInt(maxRange - minRange - queryRange) + minRange;
        int nextMax = nextMin + queryRange;
        return QueryGenerator.sid(dataverse, dataset, nextMin, nextMax, skipPkIndex);
    }

}
