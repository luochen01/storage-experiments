package edu.uci.asterixdb.storage.sim;

import edu.uci.asterixdb.storage.sim.Page.PageState;

class Page {

    public enum PageState {
        CACHED,
        CACHED_SIMULATE,
        NONE
    }

    Page prev = null;
    Page next = null;
    PageState state = PageState.NONE;

    public Page() {
    }

}

public class LRUCache {
    private final DoubleLinkedList cacheList = new DoubleLinkedList();
    private final DoubleLinkedList simulateList = new DoubleLinkedList();

    private int cacheCapacity;
    private int simulateCapacity;

    private long queryReads;
    private long queryDiskReads;
    private long mergeReads;
    private long mergeDiskReads;
    private long diskWrites;

    private long savedQueryDiskReads;
    private long savedMergeDiskReads;

    public LRUCache(int cacheCapacity, int simulateCapacity) {
        this.cacheCapacity = cacheCapacity;
        this.simulateCapacity = simulateCapacity;
    }

    public void delete(Page page) {
        switch (page.state) {
            case CACHED:
                cacheList.delete(page);
                break;
            case CACHED_SIMULATE:
                simulateList.delete(page);
                break;
            default:
                break;
        }
        page.state = PageState.NONE;
    }

    public void pin(Page page) {
        queryReads++;
        switch (page.state) {
            case CACHED:
                cacheList.makeHead(page);
                break;
            case CACHED_SIMULATE:
                queryDiskReads++;
                savedQueryDiskReads++;
                simulateList.delete(page);
                cacheList.insert(page);
                page.state = PageState.CACHED;
                ensureCacheCapacity();
                break;
            case NONE:
                queryDiskReads++;
                cacheList.insert(page);
                page.state = PageState.CACHED;
                ensureCacheCapacity();
                break;
        }
    }

    public void ensureCacheCapacity() {
        while (cacheList.getSize() > cacheCapacity) {
            Page page = cacheList.deleteLast();
            simulateList.insert(page);
            page.state = PageState.CACHED_SIMULATE;
        }
        while (simulateList.getSize() > simulateCapacity) {
            Page page = simulateList.deleteLast();
            page.state = PageState.NONE;
        }
    }

    public void mergeReturnPage(Page page) {
        assert page.state == PageState.NONE;
        cacheList.insert(page);
        page.state = PageState.CACHED;
        ensureCacheCapacity();
    }

    public void mergeReadPage(Page page) {
        mergeReads++;
        switch (page.state) {
            case CACHED:
                break;
            case CACHED_SIMULATE:
                mergeDiskReads++;
                savedMergeDiskReads++;
                break;
            case NONE:
                mergeDiskReads++;
                break;
        }

    }

    public void write(int numPages) {
        diskWrites += numPages;
    }

    public void adjustCapacity(int newCapacity) {
        cacheCapacity = newCapacity;
        ensureCacheCapacity();
    }

    public Page getPage(int index) {
        return cacheList.get(index);
    }

    public long getQueryDiskReads() {
        return queryDiskReads;
    }

    public long getMergeDiskReads() {
        return mergeDiskReads;
    }

    public long getDiskWrites() {
        return diskWrites;
    }

    public long getSavedMergeDiskReads() {
        return savedMergeDiskReads;
    }

    public long getSavedQueryDiskReads() {
        return savedQueryDiskReads;
    }

    public long getMergeReads() {
        return mergeReads;
    }

    public long getQueryReads() {
        return queryReads;
    }

    public void resetStats() {
        this.savedMergeDiskReads = 0;
        this.savedQueryDiskReads = 0;
        this.queryDiskReads = 0;
        this.mergeDiskReads = 0;
        this.queryReads = 0;
        this.mergeReads = 0;
        this.diskWrites = 0;
    }

    public int getCacheSize() {
        return cacheList.getSize();
    }

    public int getSimulateCacheSize() {
        return simulateList.getSize();
    }

}