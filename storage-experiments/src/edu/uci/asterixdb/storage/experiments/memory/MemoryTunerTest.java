package edu.uci.asterixdb.storage.experiments.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class MemoryTunerTest {

    private double kp = 1024 * 10;

    private double ki = 1024 * 10;

    private double p = 0.5;

    private double totalMemory = 1024;

    private double delta = 1;

    private double componentWeight = 0.9;

    private double cacheWeight = 0.1;

    private int historySize = 40;

    private double componentsSavedTime(double memory) {
        double a = 20;
        double b = 0.005;
        return a * (1 - Math.pow(Math.E, -b * memory));
    }

    private double componentBenefit(double memory) {
        return (componentsSavedTime(memory + delta) - componentsSavedTime(memory)) / delta;
    }

    private double componentBenefitSlope(List<Double> memories) {
        SimpleRegression regression = new SimpleRegression();

        List<Double> benefits = memories.stream().map(m -> componentBenefit(m)).collect(Collectors.toList());

        for (int i = 0; i < memories.size(); i++) {
            regression.addData(memories.get(i), benefits.get(i));
        }

        return regression.getSlope();
    }

    private double cacheSavedTime(double memory) {
        double a = 200;
        double b = 0.0001;
        return a * (1 - Math.pow(Math.E, -b * memory));
    }

    private double cacheBenefit(double memory) {
        return (cacheSavedTime(memory + delta) - cacheSavedTime(memory)) / delta;
    }

    private double cacheBenefitSlope(List<Double> memories) {
        SimpleRegression regression = new SimpleRegression();

        List<Double> benefits = memories.stream().map(m -> cacheBenefit(m)).collect(Collectors.toList());

        for (int i = 0; i < memories.size(); i++) {
            regression.addData(memories.get(i), benefits.get(i));
        }

        return regression.getSlope();
    }

    public void control(int steps) {

        double cacheMemory = totalMemory / 2;
        double componentMemory = totalMemory / 2;

        List<Double> cacheMemories = new ArrayList<>();
        List<Double> componentMemories = new ArrayList<>();

        cacheMemories.add(0.0);
        cacheMemories.add(cacheMemory);
        componentMemories.add(0.0);
        componentMemories.add(componentMemory);

        for (int i = 0; i < steps; i++) {
            System.out.println(String.format(
                    "Step: %d, Cache memory: %f, component memory: %f, cache benefit: %f, component benefit: %f, total saved time: %f",
                    i, cacheMemory, componentMemory, cacheBenefit(cacheMemory), componentBenefit(componentMemory),
                    cacheWeight * cacheSavedTime(cacheMemory)
                            + componentWeight * componentsSavedTime(componentMemory)));

            double avgBenefit =
                    (cacheWeight * cacheBenefit(cacheMemory) + componentWeight * componentBenefit(componentMemory)) / 2;

            //            double cacheError = -(avgBenefit - cacheWeight * cacheBenefit(cacheMemory));
            //            cacheMemory = cacheMemory + (ki + kp) * cacheError - kp * pCacheError;
            //            pCacheError = cacheError;
            //
            //            double componentError = -(avgBenefit - componentWeight * componentBenefit(componentMemory));
            //            componentMemory = componentMemory + (ki + kp) * componentError - kp * pComponentError;
            //            pComponentError = componentError;

            double cacheSlope = cacheBenefitSlope(cacheMemories);
            cacheMemory = cacheMemory + (p - 1) / cacheSlope * (cacheWeight * cacheBenefit(cacheMemory) - avgBenefit);

            if (cacheMemory > totalMemory) {
                cacheMemory = totalMemory;
            }
            if (cacheMemory < 0) {
                cacheMemory = 0;
            }

            double componentSlope = componentBenefitSlope(componentMemories);
            componentMemory = componentMemory
                    + (p - 1) / componentSlope * (componentWeight * componentBenefit(componentMemory) - avgBenefit);

            if (componentMemory > totalMemory) {
                componentMemory = totalMemory;
            }
            if (componentMemory < 0) {
                componentMemory = 0;
            }

            // normalize
            double cacheRatio = cacheMemory / (cacheMemory + componentMemory);
            double componentRatio = componentMemory / (cacheMemory + componentMemory);
            cacheMemory = cacheRatio * totalMemory;
            componentMemory = componentRatio * totalMemory;

            if (!equals(cacheMemory, cacheMemories.get(cacheMemories.size() - 1))) {
                cacheMemories.add(cacheMemory);
                componentMemories.add(componentMemory);
                if (cacheMemories.size() > historySize) {
                    cacheMemories.remove(0);
                    componentMemories.remove(0);
                }
            }
        }
    }

    private boolean equals(double a, double b) {
        double delta = 0.0000000001;
        return Math.abs(a - b) < delta;
    }

    public void bruteForce() {
        double currentMax = 0;
        double bestComponentMemory = 0;
        double bestCacheMemory = 0;
        for (int i = 0; i <= totalMemory; i++) {
            double savedTime = cacheWeight * cacheSavedTime(i) + componentWeight * componentsSavedTime(totalMemory - i);
            if (savedTime > currentMax) {
                currentMax = savedTime;
                bestCacheMemory = i;
                bestComponentMemory = totalMemory - i;
            }
        }
        System.out.println(String.format("Brute force: cache memory: %f, component memory: %f, total saved time: %f",
                bestCacheMemory, bestComponentMemory, currentMax));
    }

    public static void main(String[] args) {
        MemoryTunerTest tuner = new MemoryTunerTest();
        tuner.control(500);
        tuner.bruteForce();
    }

}
