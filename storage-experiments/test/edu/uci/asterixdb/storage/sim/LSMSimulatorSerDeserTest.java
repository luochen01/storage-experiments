package edu.uci.asterixdb.storage.sim;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.asterixdb.storage.sim.LSMSimulator.CachePolicy;

public class LSMSimulatorSerDeserTest {

    @Test
    public void test() throws IOException {

        int card = 100000;
        Config config = new Config(new MemoryConfig(64, 4096, 128, 10, true), new DiskConfig(128, 10, 1, false),
                new TuningConfig(4096, 64, 4, 1, 1, CachePolicy.ADAPTIVE, Integer.MAX_VALUE), card,
                GreedySelector.INSTANCE, GreedySelector.INSTANCE, 0, 0);

        UniformGenerator gen = new UniformGenerator();
        gen.initCard(card);
        LSMSimulator sim = new LSMSimulator(gen, gen, config);

        sim.load(new File("output"));

        LSMSimulator newSim = new LSMSimulator(gen, gen, config);
        newSim.deserialize(new File("output"));

        Assert.assertEquals(sim.unpartitionedLevel, newSim.unpartitionedLevel);
        Assert.assertEquals(sim.diskLevels, newSim.diskLevels);
    }

}