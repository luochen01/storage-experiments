package edu.uci.asterixdb.storage.sim.lsm.cache;

import java.util.function.Consumer;

import edu.uci.asterixdb.storage.sim.lsm.cache.Page.PageState;

public class LRUCache implements ICache {
    private Page head = new Page();
    private Page tail = new Page();

    private int size;

    private int capacity;
    private final PageState cacheState;

    public LRUCache(int capacity, PageState cacheState) {
        head.next = tail;
        tail.prev = head;
        this.cacheState = cacheState;
        this.capacity = capacity;
    }

    @Override
    public Page access(Page page) {
        if (page.state == cacheState) {
            deletePage(page);
            insertHead(page);
            page.state = cacheState;
            return null;
        } else {
            Page victim = null;
            if (size >= capacity) {
                victim = evictLast();
            } else {
                size++;
            }
            insertHead(page);
            page.state = cacheState;
            return victim;
        }
    }

    @Override
    public boolean isCached(Page page) {
        return page.state == cacheState;
    }

    @Override
    public void resize(int capacity, Consumer<Page> pageProcessor) {
        while (size > capacity) {
            Page page = evictLast();
            pageProcessor.accept(page);
            size--;
        }
    }

    private void insertHead(Page page) {
        Page next = head.next;

        head.next = page;
        page.prev = head;

        page.next = next;
        next.prev = page;
    }

    private Page evictLast() {
        Page page = tail.prev;
        deletePage(page);
        return page;
    }

    private void deletePage(Page page) {
        assert page.state == cacheState;
        Page prev = page.prev;
        Page next = page.next;
        assert prev != null;
        prev.next = next;
        next.prev = prev;

        page.reset();
    }

    @Override
    public void delete(Page page) {
        deletePage(page);
        size--;
    }

    @Override
    public int getSize() {
        return size;
    }

    public Page getPage(int index) {
        Page p = head.next;
        for (int i = 0; p != tail; i++, p = p.next) {
            if (i == index) {
                return p;
            }
        }
        return null;
    }

}
