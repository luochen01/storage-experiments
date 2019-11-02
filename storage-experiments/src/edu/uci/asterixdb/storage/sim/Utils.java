package edu.uci.asterixdb.storage.sim;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

class Utils {

    public static String formatDivision(long v1, long v2) {
        return String.format("%.2f", (double) v1 / v2);
    }

    public static Pair<Integer, Integer> findOverlappingSSTableIndex(StorageUnit sstable,
            List<? extends StorageUnit> sstables) {
        int index = Collections.binarySearch(sstables, sstable);
        if (index < 0) {
            // not found
            return null;
        } else {
            int begin = index;
            while (begin > 0 && sstables.get(begin - 1).max() >= sstable.min()) {
                begin--;
            }
            int end = index;
            while (end < sstables.size() - 1 && sstables.get(end + 1).min() <= sstable.max()) {
                end++;
            }
            return Pair.of(begin, end + 1);
        }
    }

    private static final ThreadLocal<Pair<SSTable, SSTable>> LOCAL_SSTABLES = new ThreadLocal<>();

    public static TreeSet<StorageUnit> findOverlappingSSTables(StorageUnit sstable, TreeSet<StorageUnit> sstables) {
        Pair<SSTable, SSTable> pair = LOCAL_SSTABLES.get();
        if (pair == null) {
            pair = Pair.of(new SSTable(1, false), new SSTable(1, false));
            LOCAL_SSTABLES.set(pair);
        }
        pair.getLeft().resetKey(sstable.min());
        pair.getRight().resetKey(sstable.max());
        return (TreeSet<StorageUnit>) sstables.subSet(pair.getLeft(), true, pair.getRight(), true);
    }

    public static void addLevel(List<StorageUnit> newSSTables, List<PartitionedLevel> levels, boolean isMemory) {
        levels.forEach(l -> l.level++);
        PartitionedLevel newLevel = new PartitionedLevel(0, isMemory);
        newSSTables.forEach(t -> newLevel.add(t));
        levels.add(0, newLevel);
    }

    public static long getLevelCapacity(List<PartitionedLevel> levels, int level, int sizeRatio) {
        if (level >= levels.size()) {
            throw new IllegalArgumentException();
        }
        long capacity = levels.get(levels.size() - 1).getSize();
        for (int i = levels.size() - 1; i > level; i--) {
            capacity /= sizeRatio;
        }
        return capacity;
    }

    public static <T extends StorageUnit> void replace(List<T> list, List<T> oldUnits, List<T> newUnits) {
        int index = -1;
        if (!oldUnits.isEmpty()) {
            index = Collections.binarySearch(list, oldUnits.get(0));
            assert index >= 0;
            list.subList(index, index + oldUnits.size()).clear();
        }
        if (index == -1) {
            index = Collections.binarySearch(list, newUnits.get(0));
            if (index < 0) {
                index = -index - 1;
            }
        }
        list.addAll(index, newUnits);
    }

    public static KeyEntry getNextKey(MergeIterator iterator, KeyEntry keyEntry) {
        if (iterator.hasNext()) {
            iterator.getNext(keyEntry);
            return keyEntry;
        } else {
            return null;
        }
    }

    public static int getTotalSize(Collection<? extends StorageUnit> list) {
        int total = 0;
        for (StorageUnit unit : list) {
            total += unit.getSize();
        }
        return total;
    }

    public static StorageUnit getNextMergeSSTable(TreeSet<StorageUnit> sstables, StorageUnit key) {
        StorageUnit sstable = sstables.higher(key);
        if (sstable == null) {
            return sstables.first();
        } else {
            return sstable;
        }
    }

    public static void updatePriorityQueue(PriorityQueue<SSTable> queue, List<SSTable> oldSSTables,
            List<SSTable> newSSTables) {
        queue.removeAll(oldSSTables);
        queue.addAll(newSSTables);
    }

}