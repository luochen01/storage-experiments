package edu.uci.asterixdb.storage.sim.lsm.cache;

public class Page {
    public enum PageState {
        CACHED,
        CACHED_SIMULATE,
        NONE
    }

    Page prev = null;
    Page next = null;
    public PageState state = PageState.NONE;
    int index = -1;
    boolean accessed = false;

    public Page() {
    }

    public void reset() {
        prev = null;
        next = null;
        state = PageState.NONE;
        index = -1;
        accessed = false;
    }
}
