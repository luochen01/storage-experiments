package edu.uci.asterixdb.storage.sim.lsm;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

class Utils {

    public static String formatDivision(long v1, long v2) {
        return String.format("%.2f", (double) v1 / v2);
    }

    public static Pair<Integer, Integer> findOverlappingSSTableIndex(SSTable sstable, List<SSTable> sstables) {
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

    private static final ThreadLocal<Pair<KeySSTable, KeySSTable>> LOCAL_SSTABLES = new ThreadLocal<>();

    public static NavigableSet<SSTable> findOverlappingSSTables(SSTable sstable, TreeSet<SSTable> sstables) {
        Pair<KeySSTable, KeySSTable> pair = LOCAL_SSTABLES.get();
        if (pair == null) {
            pair = Pair.of(new KeySSTable(), new KeySSTable());
            LOCAL_SSTABLES.set(pair);
        }
        pair.getLeft().resetKey(sstable.min());
        pair.getRight().resetKey(sstable.max());
        return sstables.subSet(pair.getLeft(), true, pair.getRight(), true);
    }

    public static void addLevel(List<SSTable> newSSTables, List<PartitionedLevel> levels, boolean isMemory) {
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

    public static <T extends SSTable> void replace(List<T> list, List<T> oldUnits, List<T> newUnits) {
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

    public static int getTotalSize(Collection<SSTable> list) {
        int total = 0;
        for (SSTable unit : list) {
            total += unit.getSize();
        }
        return total;
    }

    public static SSTable getNextMergeSSTable(TreeSet<SSTable> sstables, SSTable key) {
        SSTable sstable = sstables.higher(key);
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

    public static int getBloomFilterPages(int numKeys, int pageSize) {
        int bfBytes = Utils.ceil(numKeys * 10, 8);
        return Utils.ceil(bfBytes, 1024 * pageSize);
    }

    public static int ceil(int a, int b) {
        int c = a / b;
        if (a % b != 0) {
            return c + 1;
        } else {
            return c;
        }
    }

    public static boolean contains(TreeSet<SSTable> sstables, KeySSTable key) {
        SSTable sstable = sstables.ceiling(key);
        if (sstable == null || sstable.min() > key.max()) {
            return false;
        }
        return sstable.contains(key.min());
    }

}