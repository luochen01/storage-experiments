package edu.uci.asterixdb.storage.experiments.util;

/**
 * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than
 * others, according to a zipfian distribution. When you construct an instance of this class, you specify the number
 * of items in the set to draw from, either by specifying an itemcount (so that the sequence is of items from 0 to
 * itemcount-1) or by specifying a min and a max (so that the sequence is of items from min to max inclusive). After
 * you construct the instance, you can change the number of items by calling nextInt(itemcount) or nextLong(itemcount).
 * <p>
 * Unlike @ZipfianGenerator, this class scatters the "popular" items across the itemspace. Use this, instead of
 * @ZipfianGenerator, if you don't want the head of the distribution (the popular items) clustered together.
 */
public class ScrambledZipfianGenerator {
    public static final double ZETAN = 26.46902820178302;
    public static final double USED_ZIPFIAN_CONSTANT = 0.99;

    private ZipfianGenerator gen;
    private final long min, max;

    /******************************* Constructors **************************************/

    /**
     * Create a zipfian generator for the specified number of items.
     *
     * @param items
     *            The number of items in the distribution.
     */
    public ScrambledZipfianGenerator(long items) {
        this(0, items - 1);
    }

    /**
     * Create a zipfian generator for items between min and max.
     *
     * @param min
     *            The smallest integer to generate in the sequence.
     * @param max
     *            The largest integer to generate in the sequence.
     */
    public ScrambledZipfianGenerator(long min, long max) {
        this(min, max, ZipfianGenerator.ZIPFIAN_CONSTANT);
    }

    /**
     * Create a zipfian generator for the specified number of items using the specified zipfian constant.
     *
     * @param _items
     *            The number of items in the distribution.
     * @param _zipfianconstant
     *            The zipfian constant to use.
     */
    /*
    // not supported, as the value of zeta depends on the zipfian constant, and we have only precomputed zeta for one
    zipfian constant
    public ScrambledZipfianGenerator(long _items, double _zipfianconstant)
    {
    this(0,_items-1,_zipfianconstant);
    }
    */

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant. If you
     * use a zipfian constant other than 0.99, this will take a long time to complete because we need to recompute zeta.
     *
     * @param min
     *            The smallest integer to generate in the sequence.
     * @param max
     *            The largest integer to generate in the sequence.
     * @param zipfianconstant
     *            The zipfian constant to use.
     */
    public ScrambledZipfianGenerator(long min, long max, double zipfianconstant) {
        this.min = min;
        this.max = max;
        if (zipfianconstant == USED_ZIPFIAN_CONSTANT) {
            gen = new ZipfianGenerator(0, max - min, zipfianconstant, ZETAN);
        } else {
            gen = new ZipfianGenerator(0, max - min, zipfianconstant);
        }
    }

    /**************************************************************************************************/

    public long nextValue(long newItemCount) {
        long ret = gen.nextLong(newItemCount);
        ret = min + fnvhash64(ret) % newItemCount;
        return ret;
    }

    public static final long FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
    public static final long FNV_PRIME_64 = 1099511628211L;

    public static final int FNV_OFFSET_BASIS_32 = 0x811c9dc5;
    public static final int FNV_PRIME_32 = 16777619;

    /**
     * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
     *
     * @param val
     *            The value to hash.
     * @return The hash value
     */
    public static long fnvhash64(long val) {
        //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
        long hashval = FNV_OFFSET_BASIS_64;

        for (int i = 0; i < 8; i++) {
            long octet = val & 0x00ff;
            val = val >> 8;

            hashval = hashval ^ octet;
            hashval = hashval * FNV_PRIME_64;
            //hashval = hashval ^ octet;
        }
        return Math.abs(hashval);
    }

    /**
     * 32 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
     *
     * @param val
     *            The value to hash.
     * @return The hash value
     */
    public static int fnvhash32(int val) {
        //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
        int hashval = FNV_OFFSET_BASIS_32;

        for (int i = 0; i < 4; i++) {
            int octet = val & 0x00ff;
            val = val >> 4;

            hashval = hashval ^ octet;
            hashval = hashval * FNV_PRIME_32;
            //hashval = hashval ^ octet;
        }
        return Math.abs(hashval);
    }

    public static void main(String[] args) {
        long itemCount = 1000;
        int sampleCount = 10000;
        int[] counters = new int[(int) itemCount];
        ZipfianGenerator gen = new ZipfianGenerator(itemCount, 0.99);
        for (int i = 0; i < sampleCount; i++) {
            int value = (int) gen.nextLong(itemCount);
            counters[value]++;
        }
        for (int i : counters) {
            System.out.println(i);
        }
    }

}