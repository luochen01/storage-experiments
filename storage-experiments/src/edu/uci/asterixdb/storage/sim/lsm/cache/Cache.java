package edu.uci.asterixdb.storage.sim.lsm.cache;

import java.util.function.Consumer;

import edu.uci.asterixdb.storage.sim.lsm.cache.Page.PageState;

public class Cache implements ICache {

    private final ICache cache;
    private final ICache simulateCache;

    private long mergeReads;
    private long queryReads;
    private long queryDiskReads;
    private long mergeDiskReads;
    private long mergeDiskWrites;
    private long flushDiskWrites;

    private long savedQueryDiskReads;
    private long savedMergeDiskReads;

    public Cache(ICache cache, ICache simulateCache) {
        this.cache = cache;
        this.simulateCache = simulateCache;
    }

    @Override
    public void delete(Page page) {
        switch (page.state) {
            case CACHED:
                cache.delete(page);
                break;
            case CACHED_SIMULATE:
                simulateCache.delete(page);
                break;
            default:
                break;
        }
        assert page.state == PageState.NONE;
    }

    @Override
    public Page access(Page page) {
        queryReads++;
        if (!cache.isCached(page)) {
            queryDiskReads++;
            if (simulateCache.isCached(page)) {
                simulateCache.delete(page);
                savedQueryDiskReads++;
            }
            Page victim = cache.access(page);
            if (victim != null) {
                simulateCache.access(victim);
            }
            return victim;
        } else {
            Page victim = cache.access(page);
            assert victim == null;
            return victim;
        }
    }

    @Override
    public boolean isCached(Page page) {
        return cache.isCached(page);
    }

    @Override
    public void resize(int capacity, Consumer<Page> pageProcessor) {
        cache.resize(capacity, p -> simulateCache.access(p));
    }

    public void mergeReturnPage(Page page) {
        if (page.state == PageState.CACHED_SIMULATE) {
            simulateCache.delete(page);
        }
        cache.access(page);
    }

    public void mergeReadPage(Page page) {
        mergeReads++;
        if (!cache.isCached(page)) {
            mergeDiskReads++;
            if (simulateCache.isCached(page)) {
                savedMergeDiskReads++;
            }
        }
    }

    public void mergeWrite(int numPages) {
        mergeDiskWrites += numPages;
    }

    public void flushWrite(int numPages) {
        flushDiskWrites += numPages;
    }

    public long getQueryDiskReads() {
        return queryDiskReads;
    }

    public long getMergeDiskReads() {
        return mergeDiskReads;
    }

    public long getDiskReads() {
        return mergeDiskReads + queryDiskReads;
    }

    public long getDiskWrites() {
        return mergeDiskWrites + flushDiskWrites;
    }

    public long getMergeDiskWrites() {
        return mergeDiskWrites;
    }

    public long getFlushDiskWrites() {
        return flushDiskWrites;
    }

    public long getSavedMergeDiskReads() {
        return savedMergeDiskReads;
    }

    public long getSavedQueryDiskReads() {
        return savedQueryDiskReads;
    }

    public long getQueryReads() {
        return queryReads;
    }

    public long getMergeReads() {
        return mergeReads;
    }

    public void resetStats() {
        this.savedMergeDiskReads = 0;
        this.savedQueryDiskReads = 0;
        this.queryDiskReads = 0;
        this.mergeDiskReads = 0;
        this.queryReads = 0;
        this.mergeDiskWrites = 0;
        this.flushDiskWrites = 0;
    }

    public int getCacheSize() {
        return cache.getSize();
    }

    public int getSimulateCacheSize() {
        return simulateCache.getSize();
    }

    @Override
    public int getSize() {
        return cache.getSize();
    }

}