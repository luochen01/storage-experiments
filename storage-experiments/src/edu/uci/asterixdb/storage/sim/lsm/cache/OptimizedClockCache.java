package edu.uci.asterixdb.storage.sim.lsm.cache;

import edu.uci.asterixdb.storage.sim.lsm.cache.Page.PageState;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;

public class OptimizedClockCache extends ClockCache {

    private IntArrayFIFOQueue deletedPageIds = new IntArrayFIFOQueue();

    public OptimizedClockCache(int capacity, PageState cacheState) {
        super(capacity, cacheState);
    }

    @Override
    public void delete(Page page) {
        assert page.index >= 0;
        deletedPageIds.enqueue(page.index);
        super.delete(page);
    }

    @Override
    protected int findVictim() {
        if (!deletedPageIds.isEmpty()) {
            return deletedPageIds.dequeueInt();
        } else {
            return super.findVictim();
        }
    }
}
