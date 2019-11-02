package edu.uci.asterixdb.storage.sim;

import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;

import com.yahoo.ycsb.Utils;

interface KeyGenerator {
    public int nextKey();

    public void initCard(int card);

    public KeyGenerator clone();
}

class UniformGenerator implements KeyGenerator {
    private final Random rand = new Random(17);

    private int card;

    public UniformGenerator() {
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

class ScrambleZipfGenerator implements KeyGenerator {
    private ZipfDistribution zipf;
    private int cardinality;

    public ScrambleZipfGenerator() {
    }

    @Override
    public void initCard(int card) {
        this.cardinality = card;
        this.zipf = new ZipfDistribution(cardinality, 0.99);
    }

    @Override
    public int nextKey() {
        int value = zipf.sample();
        return (int) (Utils.fnvhash64(value) % cardinality);
    }

    @Override
    public String toString() {
        return "scrable-zipf";
    }

    @Override
    public ScrambleZipfGenerator clone() {
        return new ScrambleZipfGenerator();
    }
}