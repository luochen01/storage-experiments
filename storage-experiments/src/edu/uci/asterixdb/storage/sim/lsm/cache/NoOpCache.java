package edu.uci.asterixdb.storage.sim.lsm.cache;

import java.util.function.Consumer;

public class NoOpCache implements ICache {

    @Override
    public Page access(Page page) {
        return null;
    }

    @Override
    public boolean isCached(Page page) {
        return false;
    }

    @Override
    public void delete(Page page) {

    }

    @Override
    public void resize(int capacity, Consumer<Page> pageProcessor) {

    }

    @Override
    public int getSize() {
        return 0;
    }

}
