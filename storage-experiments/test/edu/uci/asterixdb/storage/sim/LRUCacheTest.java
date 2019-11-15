package edu.uci.asterixdb.storage.sim;

import org.junit.Assert;
import org.junit.Test;

public class LRUCacheTest {

    @Test
    public void test() {
        LRUCache cache = new LRUCache(5, 4);

        Page page0 = new Page();
        cache.pin(page0);
        Assert.assertEquals(page0, cache.get(0));

        Page page1 = new Page();
        cache.pin(page1);
        Assert.assertEquals(page1, cache.get(0));
        Assert.assertEquals(page0, cache.get(1));

        Page page2 = new Page();
        cache.pin(page2);
        cache.pin(page1);
        Assert.assertEquals(page1, cache.get(0));

        Page page3 = new Page();
        cache.pin(page3);

        Page page4 = new Page();
        cache.pin(page4);

        Page page5 = new Page();
        cache.pin(page5);

        Assert.assertEquals(5, cache.getUsage());

        Assert.assertFalse(page0.cached);

        cache.pin(page0);
        Assert.assertEquals(page0, cache.get(0));

        Assert.assertFalse(page2.cached);
    }

}