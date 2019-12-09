package edu.uci.asterixdb.storage.sim;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.XYChart;

import com.yahoo.ycsb.Utils;

class SeedGenerator {
    private static int SEED = 0;

    public static int getSeed() {
        return SEED++;
    }
}

interface KeyGenerator {

    public int nextKey();

    public void initCard(int card);

    public KeyGenerator clone();

}

class UniformGenerator implements KeyGenerator {
    private final Random rand;
    private int card;

    public UniformGenerator() {
        rand = new Random(SeedGenerator.getSeed());
    }

    @Override
    public void initCard(int card) {
        this.card = card;

    }

    @Override
    public int nextKey() {
        return rand.nextInt(card);
    }

    @Override
    public String toString() {
        return "uniform";
    }

    @Override
    public UniformGenerator clone() {
        return new UniformGenerator();
    }
}

class RangeSkewGenerator implements KeyGenerator {
    private final int hot;
    private final Random rand;
    private int card;

    public RangeSkewGenerator(int hot) {
        this.hot = hot;
        rand = new Random(SeedGenerator.getSeed());
    }

    @Override
    public RangeSkewGenerator clone() {
        return new RangeSkewGenerator(hot);
    }

    @Override
    public void initCard(int card) {
        this.card = card;
    }

    @Override
    public int nextKey() {
        if (rand.nextInt(100) < hot) {
            return rand.nextInt(card / 2);
        } else {
            return rand.nextInt(card / 2) + card;
        }
    }

}

class ZipfGenerator implements KeyGenerator {
    private ZipfDistribution zipf;

    public ZipfGenerator() {
    }

    @Override
    public void initCard(int card) {
        this.zipf = new ZipfDistribution(card, 0.99);
    }

    @Override
    public int nextKey() {
        return zipf.sample() - 1;
    }

    @Override
    public String toString() {
        return "zipf";
    }

    @Override
    public ZipfGenerator clone() {
        return new ZipfGenerator();
    }

}

public class Gen {

    public static void main(String[] args) throws IOException {
        double[] keys = new double[1000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = i + 1;
        }
        double[] nums = new double[keys.length];

        int total = 100000;

        ZipfGenerator gen = new ZipfGenerator();
        gen.initCard(nums.length);
        for (int i = 0; i < total; i++) {
            nums[gen.nextKey()] += (1.0 / total);
        }
        double[] sums = new double[nums.length];
        sums[0] = nums[0];
        for (int i = 1; i < nums.length; i++) {
            sums[i] += sums[i - 1] + nums[i];
        }
        XYChart chart = QuickChart.getChart("Cumulative Distribution Function", "key", "CDF", "y(x)", keys, sums);

        BitmapEncoder.saveBitmapWithDPI(chart, "./cdf.png", BitmapFormat.PNG, 300);

        chart = QuickChart.getChart("Probability Density Function", "key", "PDF", "y(x)", keys, nums);
        BitmapEncoder.saveBitmapWithDPI(chart, "./pdf.png", BitmapFormat.PNG, 300);
    }
}

class ScrambleZipfGenerator implements KeyGenerator {
    private ZipfDistribution zipf;
    private final RandomGenerator gen;
    private int cardinality;

    public ScrambleZipfGenerator() {
        this.gen = new JDKRandomGenerator(SeedGenerator.getSeed());
    }

    @Override
    public void initCard(int card) {
        this.cardinality = card;
        this.zipf = new ZipfDistribution(gen, cardinality, 0.99);
    }

    @Override
    public int nextKey() {
        int value = zipf.sample();
        return (int) (Utils.fnvhash64(value) % cardinality);
    }

    @Override
    public String toString() {
        return "scrablezipf";
    }

    @Override
    public ScrambleZipfGenerator clone() {
        return new ScrambleZipfGenerator();
    }
}