package edu.uci.asterixdb.storage.sim;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

class KeyEntry {
    int key;
    int seq;
    boolean cached;
}

interface MergeIterator {
    public boolean hasNext();

    public void getNext(KeyEntry entry);
}

class PQEntry implements Comparable<PQEntry> {
    final Iterator<SSTable> sstableIterator;

    SSTable sstable;
    private int keyIndex;

    final int pos;

    public PQEntry(Collection<SSTable> treeSet, int pos) {
        sstableIterator = treeSet.iterator();
        if (sstableIterator.hasNext()) {
            sstable = sstableIterator.next();
        } else {
            sstable = null;
        }
        this.pos = pos;

        treeSet.forEach(t -> t.readAll());
    }

    public PQEntry(SSTable sstable, int pos) {
        this.sstable = sstable;
        this.sstableIterator = null;
        this.pos = pos;

        sstable.readAll();
    }

    public int getKey() {
        return sstable.getKey(keyIndex);
    }

    public int getSeq() {
        return sstable.getSeq(keyIndex);
    }

    public boolean isCached() {
        return sstable.isCached(keyIndex);
    }

    public void consumeKey() {
        keyIndex++;
        if (keyIndex == sstable.getSize()) {
            keyIndex = 0;
            if (sstableIterator != null && sstableIterator.hasNext()) {
                sstable = sstableIterator.next();
            } else {
                sstable = null;
            }
        }
    }

    public boolean hasMore() {
        return (sstable != null && keyIndex < sstable.getSize())
                || (sstableIterator != null && sstableIterator.hasNext());
    }

    @Override
    public int compareTo(PQEntry o) {
        int cmp = Long.compare(getKey(), o.getKey());
        if (cmp != 0) {
            return cmp;
        } else {
            return Integer.compare(pos, o.pos);
        }
    }
}

abstract class AbstractIterator implements MergeIterator {
    protected final PriorityQueue<PQEntry> queue = new PriorityQueue<>();

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public void getNext(KeyEntry entry) {
        PQEntry peek = queue.peek();
        entry.key = peek.getKey();
        entry.seq = peek.getSeq();
        entry.cached = peek.isCached();

        while (!queue.isEmpty() && queue.peek().getKey() == entry.key) {
            consumeMin();
        }

    }

    protected void consumeMin() {
        PQEntry entry = queue.poll();
        entry.consumeKey();
        if (entry.hasMore()) {
            queue.add(entry);
        }
    }

}

class DiskFlushIterator extends AbstractIterator {

    public DiskFlushIterator(List<List<SSTable>> sstables) {
        int pos = 0;
        for (List<SSTable> list : sstables) {
            if (!list.isEmpty()) {
                queue.add(new PQEntry(list, pos++));
            }
        }
    }
}

class UnpartitionedIterator extends AbstractIterator {
    public UnpartitionedIterator(List<SSTable> unpartitioned, Set<SSTable> partitioned) {
        int pos = 0;
        for (SSTable sstable : unpartitioned) {
            queue.add(new PQEntry(sstable, pos++));
        }
        if (!partitioned.isEmpty()) {
            queue.add(new PQEntry(partitioned, pos++));
        }
    }
}

class PartitionedIterator extends AbstractIterator {

    public PartitionedIterator(SSTable currentLevel, Set<SSTable> nextLevel) {
        queue.add(new PQEntry(currentLevel, 0));
        if (!nextLevel.isEmpty()) {
            queue.add(new PQEntry(nextLevel, 1));
        }
    }

}
