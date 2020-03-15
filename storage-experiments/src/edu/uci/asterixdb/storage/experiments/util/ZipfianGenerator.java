package edu.uci.asterixdb.storage.experiments.util;

import java.util.Random;

/**
 * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than
 * others, according to a zipfian distribution. When you construct an instance of this class, you specify the number
 * of items in the set to draw from, either by specifying an itemcount (so that the sequence is of items from 0 to
 * itemcount-1) or by specifying a min and a max (so that the sequence is of items from min to max inclusive). After
 * you construct the instance, you can change the number of items by calling nextInt(itemcount) or nextLong(itemcount).
 *
 * Note that the popular items will be clustered together, e.g. item 0 is the most popular, item 1 the second most
 * popular, and so on (or min is the most popular, min+1 the next most popular, etc.) If you don't want this clustering,
 * and instead want the popular items scattered throughout the item space, then use ScrambledZipfianGenerator instead.
 *
 * Be aware: initializing this generator may take a long time if there are lots of items to choose from (e.g. over a
 * minute for 100 million objects). This is because certain mathematical values need to be computed to properly
 * generate a zipfian skew, and one of those values (zeta) is a sum sequence from 1 to n, where n is the itemcount.
 * Note that if you increase the number of items in the set, we can compute a new zeta incrementally, so it should be
 * fast unless you have added millions of items. However, if you decrease the number of items, we recompute zeta from
 * scratch, so this can take a long time.
 *
 * The algorithm used here is from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994.
 */
public class ZipfianGenerator {
    public static final double ZIPFIAN_CONSTANT = 0.99;

    private final Random random = new Random(17);

    /**
     * Number of items.
     */
    private final long items;

    /**
     * Min item to generate.
     */
    private final long base;

    /**
     * The zipfian constant to use.
     */
    private final double zipfianconstant;

    /**
     * Computed parameters for generating the distribution.
     */
    private final double alpha;

    private double zetan;

    private double eta;

    private final double theta;

    private final double zeta2theta;

    /**
     * The number of items used to compute zetan the last time.
     */
    private long countforzeta;

    /******************************* Constructors **************************************/

    /**
     * Create a zipfian generator for the specified number of items.
     *
     * @param items
     *            The number of items in the distribution.
     */
    public ZipfianGenerator(long items) {
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
    public ZipfianGenerator(long min, long max) {
        this(min, max, ZIPFIAN_CONSTANT);
    }

    /**
     * Create a zipfian generator for the specified number of items using the specified zipfian constant.
     *
     * @param items
     *            The number of items in the distribution.
     * @param zipfianconstant
     *            The zipfian constant to use.
     */
    public ZipfianGenerator(long items, double zipfianconstant) {
        this(0, items - 1, zipfianconstant);
    }

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant.
     *
     * @param min
     *            The smallest integer to generate in the sequence.
     * @param max
     *            The largest integer to generate in the sequence.
     * @param zipfianconstant
     *            The zipfian constant to use.
     */
    public ZipfianGenerator(long min, long max, double zipfianconstant) {
        this(min, max, zipfianconstant, zetastatic(max - min + 1, zipfianconstant));
    }

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant, using
     * the precomputed value of zeta.
     *
     * @param min
     *            The smallest integer to generate in the sequence.
     * @param max
     *            The largest integer to generate in the sequence.
     * @param zipfianconstant
     *            The zipfian constant to use.
     * @param zetan
     *            The precomputed zeta constant.
     */
    public ZipfianGenerator(long min, long max, double zipfianconstant, double zetan) {

        items = max - min + 1;
        base = min;
        this.zipfianconstant = zipfianconstant;

        theta = this.zipfianconstant;

        zeta2theta = zeta(2, theta);

        alpha = 1.0 / (1.0 - theta);
        this.zetan = zetan;
        countforzeta = items;
        eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / this.zetan);

        nextValue();
    }

    /**************************************************************************/

    /**
     * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items,
     * using the zipfian constant thetaVal. Remember the value of n, so if we change the itemcount, we can recompute zeta.
     *
     * @param n
     *            The number of items to compute zeta over.
     * @param thetaVal
     *            The zipfian constant.
     */
    double zeta(long n, double thetaVal) {
        countforzeta = n;
        return zetastatic(n, thetaVal);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items,
     * using the zipfian constant theta. This is a static version of the function which will not remember n.
     *
     * @param n
     *            The number of items to compute zeta over.
     * @param theta
     *            The zipfian constant.
     */
    static double zetastatic(long n, double theta) {
        return zetastatic(0, n, theta, 0);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
     * has n items now but used to have st items. Use the zipfian constant thetaVal. Remember the new value of
     * n so that if we change the itemcount, we'll know to recompute zeta.
     *
     * @param st
     *            The number of items used to compute the last initialsum
     * @param n
     *            The number of items to compute zeta over.
     * @param thetaVal
     *            The zipfian constant.
     * @param initialsum
     *            The value of zeta we are computing incrementally from.
     */
    double zeta(long st, long n, double thetaVal, double initialsum) {
        countforzeta = n;
        return zetastatic(st, n, thetaVal, initialsum);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
     * has n items now but used to have st items. Use the zipfian constant theta. Remember the new value of
     * n so that if we change the itemcount, we'll know to recompute zeta.
     *
     * @param st
     *            The number of items used to compute the last initialsum
     * @param n
     *            The number of items to compute zeta over.
     * @param theta
     *            The zipfian constant.
     * @param initialsum
     *            The value of zeta we are computing incrementally from.
     */
    static double zetastatic(long st, long n, double theta, double initialsum) {
        double sum = initialsum;
        for (long i = st; i < n; i++) {

            sum += 1 / (Math.pow(i + 1, theta));
        }

        //System.out.println("countforzeta="+countforzeta);

        return sum;
    }

    /****************************************************************************************/

    /**
     * Generate the next item as a long.
     *
     * @param itemcount
     *            The number of items in the distribution.
     * @return The next item in the sequence.
     */
    public long nextLong(long itemcount) {
        //from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994

        if (itemcount != countforzeta) {
            //have to recompute zetan and eta, since they depend on itemcount
            synchronized (this) {
                if (itemcount > countforzeta) {
                    //we have added more items. can compute zetan incrementally, which is cheaper
                    zetan = zeta(countforzeta, itemcount, theta, zetan);
                    eta = (1 - Math.pow(2.0 / itemcount, 1 - theta)) / (1 - zeta2theta / zetan);
                } else if (itemcount < countforzeta) {
                    throw new IllegalStateException("Illegal decreasing item count " + itemcount);
                }
            }
        }

        double u = random.nextDouble();
        double uz = u * zetan;

        if (uz < 1.0) {
            return base;
        }

        if (uz < 1.0 + Math.pow(0.5, theta)) {
            return base + 1;
        }

        long ret = base + (long) ((itemcount) * Math.pow(eta * u - eta + 1, alpha));
        return ret;
    }

    public long nextValue() {
        return nextLong(items);
    }

    public static void main(String[] args) {
        int count = 1000;
        int total = count * 100;
        int[] values = new int[count];
        ZipfianGenerator gen = new ZipfianGenerator(count, 0.75);

        for (int i = 0; i < total; i++) {
            long value = gen.nextLong(count);
            values[(int) value]++;
        }
        int sum = 0;
        for (int i = 0; i < values.length; i++) {
            sum += values[i];
            System.out.println(i + "\t" + (double) sum / total);
        }
    }

}