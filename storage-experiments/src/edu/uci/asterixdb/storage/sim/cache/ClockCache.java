package edu.uci.asterixdb.storage.sim.cache;

import java.util.Arrays;
import java.util.function.Consumer;

import edu.uci.asterixdb.storage.sim.cache.Page.PageState;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;

public class ClockCache implements ICache {

    private Page[] pages;

    private final PageState cacheState;
    private int capacity;
    private int size = 0;
    private int clockPtr = 0;
    private int pageIdCounter = 0;
    private IntArrayFIFOQueue freePageIds = new IntArrayFIFOQueue();

    public ClockCache(int capacity, PageState cacheState) {
        this.pages = new Page[capacity];
        this.capacity = capacity;
        this.cacheState = cacheState;
    }

    @Override
    public Page access(Page page) {
        if (page.state == cacheState) {
            page.accessed = true;
            return null;
        } else {
            int index = size < capacity ? allocatePage() : findVictim();
            Page victim = pages[index];
            pages[index] = page;
            page.index = index;
            page.accessed = true;
            page.state = cacheState;
            if (victim != null && victim.state == cacheState) {
                victim.reset();
            }
            return victim;
        }
    }

    @Override
    public boolean isCached(Page page) {
        return page.state == cacheState;
    }

    protected int allocatePage() {
        size++;
        if (freePageIds.isEmpty()) {
            return pageIdCounter++;
        } else {
            return freePageIds.dequeueInt();
        }
    }

    protected int findVictim() {
        while (true) {
            clockPtr = (clockPtr + 1) % pages.length;
            if (pages[clockPtr] == null) {
                continue;
            } else if (pages[clockPtr].state != cacheState || !pages[clockPtr].accessed) {
                return clockPtr;
            } else {
                pages[clockPtr].accessed = false;
            }
        }
    }

    @Override
    public void delete(Page page) {
        // we leave the clock replacement policy to reclaim this page
        if (page.state == cacheState) {
            page.reset();
        }
    }

    @Override
    public void resize(int capacity, Consumer<Page> pageProcessor) {
        if (capacity > pages.length) {
            this.pages = Arrays.copyOf(pages, capacity);
        } else {
            while (this.size > capacity) {
                int index = findVictim();
                Page page = pages[index];
                freePageIds.enqueue(index);
                size--;
                if (page.state == cacheState) {
                    pageProcessor.accept(pages[index]);
                    pages[index].reset();
                }
            }
        }
        this.capacity = capacity;
    }

    @Override
    public int getSize() {
        return size;
    }

}
