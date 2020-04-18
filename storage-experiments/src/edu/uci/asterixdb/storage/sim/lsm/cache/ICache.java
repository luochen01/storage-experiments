package edu.uci.asterixdb.storage.sim.lsm.cache;

import java.util.function.Consumer;

public interface ICache {

    Page access(Page page);

    boolean isCached(Page page);

    void delete(Page page);

    void resize(int capacity, Consumer<Page> pageProcessor);

    int getSize();
}
