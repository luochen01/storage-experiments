package edu.uci.asterixdb.storage.sim;

class Page {
    Page prev = null;
    Page next = null;
    boolean cached = false;

    public Page() {
    }

}

public class LRUCache {
    private Page head = new Page();
    private Page tail = new Page();

    private int usage;
    private int capacity;
    private final int pageSize;

    private long diskReads;
    private long diskWrites;

    public LRUCache(int capacity, int pageSize) {
        this.capacity = capacity;
        this.pageSize = pageSize;
        head.next = tail;
        tail.prev = head;
    }

    public int getPageSize() {
        return pageSize;
    }

    private void insert(Page page) {
        Page next = head.next;

        head.next = page;
        page.prev = head;

        page.next = next;
        next.prev = page;
    }

    private void delete(Page page) {
        Page prev = page.prev;
        Page next = page.next;
        assert prev != null;
        prev.next = next;
        next.prev = prev;

        page.prev = null;
        page.next = null;
    }

    public void evict(Page page) {
        if (page.cached) {
            delete(page);
            page.cached = false;
            usage--;
        }

    }

    public void pin(Page page) {
        if (page.cached) {
            delete(page);
            insert(page);
        } else {
            diskReads++;
            insert(page);
            page.cached = true;
            if (usage == capacity) {
                Page last = tail.prev;
                delete(last);
                last.cached = false;
            } else {
                usage++;
            }
        }
    }

    public void read(Page page) {
        if (!page.cached) {
            diskReads++;
        }
    }

    public void write(int numPages) {
        diskWrites += numPages;
    }

    public void adjustCapacity(int newCapacity) {
        while (capacity > newCapacity) {
            delete(tail.prev);
            capacity--;
        }
        capacity = newCapacity;
    }

    public Page get(int index) {
        Page p = head.next;
        for (int i = 0; p != tail; i++, p = p.next) {
            if (i == index) {
                return p;
            }
        }
        return null;
    }

    public long getDiskReads() {
        return diskReads;
    }

    public long getDiskWrites() {
        return diskWrites;
    }

    public int getUsage() {
        return usage;
    }

    public void resetStats() {
        this.diskReads = 0;
        this.diskWrites = 0;
    }

}