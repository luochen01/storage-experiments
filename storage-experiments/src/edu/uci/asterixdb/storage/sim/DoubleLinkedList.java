package edu.uci.asterixdb.storage.sim;

public class DoubleLinkedList {
    private Page head = new Page();
    private Page tail = new Page();

    private int size;

    public DoubleLinkedList() {
        head.next = tail;
        tail.prev = head;
    }

    public void insert(Page page) {
        Page next = head.next;

        head.next = page;
        page.prev = head;

        page.next = next;
        next.prev = page;

        size++;
    }

    public void makeHead(Page page) {
        delete(page);
        insert(page);
    }

    public Page deleteLast() {
        Page page = tail.prev;
        delete(page);
        return page;
    }

    public void delete(Page page) {
        Page prev = page.prev;
        Page next = page.next;
        assert prev != null;
        prev.next = next;
        next.prev = prev;

        page.prev = null;
        page.next = null;

        size--;
    }

    public int getSize() {
        return size;
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

}
