package edu.uci.asterixdb.storage.sim;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.asterixdb.storage.sim.Page.PageState;

public class LRUCacheTest {

    @Test
    public void test() {
        LRUCache cache = new LRUCache(5, 5);

        Page page0 = new Page();
        cache.pin(page0);
        Assert.assertEquals(page0, cache.getPage(0));

        Page page1 = new Page();
        cache.pin(page1);
        Assert.assertEquals(page1, cache.getPage(0));
        Assert.assertEquals(page0, cache.getPage(1));

        Page page2 = new Page();
        cache.pin(page2);
        cache.pin(page1);
        Assert.assertEquals(page1, cache.getPage(0));

        Page page3 = new Page();
        cache.pin(page3);

        Page page4 = new Page();
        cache.pin(page4);

        Page page5 = new Page();
        cache.pin(page5);

        Assert.assertEquals(5, cache.getCacheSize());

        Assert.assertNotEquals(PageState.CACHED, page0.state);

        cache.pin(page0);
        Assert.assertEquals(page0, cache.getPage(0));

        Assert.assertNotEquals(PageState.CACHED, page2.state);

    }

}