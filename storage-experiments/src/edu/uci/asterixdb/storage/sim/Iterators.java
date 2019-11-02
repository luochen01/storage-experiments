package edu.uci.asterixdb.storage.sim;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

class KeyEntry {
    long key;
    long seq;
}

interface MergeIterator {
    public boolean hasNext();

    public void getNext(KeyEntry entry);
}

class PQEntry implements Comparable<PQEntry> {
    final Iterator<StorageUnit> sstableIterator;

    SSTable sstable;
    private int keyIndex;

    final int pos;

    public PQEntry(Set<StorageUnit> treeSet, int pos) {
        sstableIterator = treeSet.iterator();
        if (sstableIterator.hasNext()) {
            sstable = (SSTable) sstableIterator.next();
        } else {
            sstable = null;
        }
        this.pos = pos;
    }

    public PQEntry(StorageUnit sstable, int pos) {
        this.sstable = (SSTable) sstable;
        this.sstableIterator = null;
        this.pos = pos;
    }

    public long getKey() {
        return sstable.keys[keyIndex];
    }

    public long getSeq() {
        return sstable.seqs[keyIndex];
    }

    public void consumeKey() {
        keyIndex++;
        if (keyIndex == sstable.getSize()) {
            keyIndex = 0;
            if (sstableIterator != null && sstableIterator.hasNext()) {
                sstable = (SSTable) sstableIterator.next();
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

    public DiskFlushIterator(SSTable sstable) {
        queue.add(new PQEntry(sstable, 0));
    }
}

class UnpartitionedIterator extends AbstractIterator {

    public UnpartitionedIterator(List<StorageUnit> unpartitioned, Set<StorageUnit> partitioned) {
        int pos = 0;
        for (StorageUnit sstable : unpartitioned) {
            queue.add(new PQEntry(sstable, pos++));
        }
        if (!partitioned.isEmpty()) {
            queue.add(new PQEntry(partitioned, pos++));
        }
    }

}

class PartitionedIterator extends AbstractIterator {

    public PartitionedIterator(StorageUnit currentLevel, Set<StorageUnit> nextLevel) {
        queue.add(new PQEntry(currentLevel, 0));
        if (!nextLevel.isEmpty()) {
            queue.add(new PQEntry(nextLevel, 1));
        }
    }

}
