package edu.uci.asterixdb.storage.experiments.memory;

import static edu.uci.asterixdb.storage.experiments.memory.LSMMemoryUtils.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MultiLSMThroughputProcessor {

    private final static String basePathSecondary =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/multi-sk";

    private final static String basePathMultiLSM =
            "/Users/luochen/Documents/Research/experiments/results/memory/write/multi-lsm";

    //private final String basePath = "/tmp";

    private static final String portion = "0.4,0.4,0.025,0.025,0.025,0.025,0.025,0.025,0.025,0.025";
    private static final String[] portions10 = new String[] { "0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1",
            "0.15,0.15,0.15,0.15,0.0667,0.0667,0.0667,0.0667,0.0667,0.0667",
            "0.233,0.233,0.233,0.04286,0.04286,0.04286,0.04286,0.04286,0.04286,0.04286",
            "0.4,0.4,0.025,0.025,0.025,0.025,0.025,0.025,0.025,0.025",
            "0.9,0.011,0.011,0.011,0.011,0.011,0.011,0.011,0.011,0.011" };
    private static final String[] portionNames = new String[] { "50-50", "60-40", "70-30", "80-20", "90-10" };

    private static final String[] fields = new String[] { "0.150,0.150,0.150,0.150,0.067,0.067,0.067,0.067,0.067,0.067",
            "0.300,0.300,0.300,0.300,0.133,0.133,0.133,0.133,0.133,0.133",
            "0.450,0.450,0.450,0.450,0.200,0.200,0.200,0.200,0.200,0.200",
            "0.600,0.600,0.600,0.600,0.267,0.267,0.267,0.267,0.267,0.267",
            "0.750,0.750,0.750,0.750,0.333,0.333,0.333,0.333,0.333,0.333" };

    private static final String[] dataWorkloads =
            new String[] { "write-10-skew-2", "write-10-skew-4", "write-10-skew-6", "write-10-skew-8" };

    private static final String[] fieldNames = new String[] { "1", "2", "3", "4", "5" };

    private static final String[] POLICIES = { "maxmemory", "minlsn", "adaptive" };
    private static final String[] BTREE_POLICIES = { "maxmemory" };

    private final String basePath;
    private final String pattern;
    private final String name;
    private final String[] portions;
    private final String[] names;
    private final String[] policies;

    public MultiLSMThroughputProcessor(String basePath, String name, String pattern, String[] portions, String[] names,
            String[] policies) {
        this.name = name;
        this.basePath = basePath;
        this.pattern = pattern;
        this.portions = portions;
        this.names = names;
        this.policies = policies;
    }

    public void runVarSkew() throws IOException {
        int[][] results = new int[portions.length][policies.length];
        for (int i = 0; i < portions.length; i++) {
            for (int j = 0; j < policies.length; j++) {
                try {
                    File file = getFile(basePath, pattern, 1024, policies[j], portions[i]);
                    results[i][j] = computeThroughput(file);
                } catch (Exception e) {
                    results[i][j] = 0;
                }
            }
        }
        System.out.println(name);

        System.out.print("operation\t");
        for (String policy : policies) {
            System.out.print(name + "-" + policy + "\t");
        }
        System.out.println();

        for (int i = 0; i < results.length; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(names[i] + "\t" + LSMMemoryUtils.toString(results[i], "\t"));
            System.out.println(sb.toString());
        }
    }

    public void runVarMemory() throws IOException {
        int[][] results = new int[memories.length][policies.length];
        for (int i = 0; i < memories.length; i++) {
            for (int j = 0; j < policies.length; j++) {
                File file = getFile(basePath, pattern, memories[i], policies[j], portion);
                results[i][j] = computeThroughput(file);
            }
        }
        System.out.println(name);
        System.out.print("memory\t");
        for (String policy : policies) {
            System.out.print(name + "-" + policy + "\t");
        }
        System.out.println();
        for (int i = 0; i < results.length; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(memories[i] + "\t");
            for (int result : results[i]) {
                sb.append(result + "\t");
            }
            System.out.println(sb.toString());
        }
    }

    public void runVarData() throws IOException {
        int[][] results = new int[dataWorkloads.length][policies.length];
        for (int i = 0; i < dataWorkloads.length; i++) {
            for (int j = 0; j < policies.length; j++) {
                File file = getFile(basePath, pattern, dataWorkloads[i], 1024, policies[j], portion);
                results[i][j] = computeThroughput(file);
            }
        }
        System.out.println(name);
        System.out.print("memory\t");
        for (String policy : policies) {
            System.out.print(name + "-" + policy + "\t");
        }
        System.out.println();
        for (int i = 0; i < results.length; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(dataWorkloads[i] + "\t");
            for (int result : results[i]) {
                sb.append(result + "\t");
            }
            System.out.println(sb.toString());
        }
    }

    public static void mainSK() throws IOException {
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-static-default", "write-static-8-write-sk",
                portions10, portionNames, new String[] { "" }).runVarMemory();
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-static-tuned", "write-static-1-write-sk", portions10,
                portionNames, new String[] { "" }).runVarMemory();
        new MultiLSMThroughputProcessor(basePathSecondary, "partition", "write-partition-write-sk", portions10,
                portionNames, POLICIES).runVarMemory();
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-dynamic", "write-btree-write-sk", portions10,
                portionNames, BTREE_POLICIES).runVarMemory();

        new MultiLSMThroughputProcessor(basePathSecondary, "btree-static-default", "write-static-8-write-sk",
                portions10, portionNames, new String[] { "" }).runVarSkew();
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-static-tuned", "write-static-1-write-sk", portions10,
                portionNames, new String[] { "" }).runVarSkew();
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-dynamic", "write-btree-write-sk", portions10,
                portionNames, BTREE_POLICIES).runVarSkew();
        new MultiLSMThroughputProcessor(basePathSecondary, "partition", "write-partition-write-sk", portions10,
                portionNames, POLICIES).runVarSkew();

        new MultiLSMThroughputProcessor(basePathSecondary, "btree-static-default", "write-static-8-write-sk", fields,
                fieldNames, new String[] { "" }).runVarSkew();
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-static-tuned", "write-static-1-write-sk", fields,
                fieldNames, new String[] { "" }).runVarSkew();
        new MultiLSMThroughputProcessor(basePathSecondary, "btree-dynamic", "write-btree-write-sk", fields, fieldNames,
                BTREE_POLICIES).runVarSkew();
        new MultiLSMThroughputProcessor(basePathSecondary, "partition", "write-partition-write-sk", fields, fieldNames,
                POLICIES).runVarSkew();
    }

    public static void mainMultiLSM() throws IOException {
        new MultiLSMThroughputProcessor(basePathMultiLSM, "btree-static-default", "write-static-8-write-10", portions10,
                portionNames, new String[] { "" }).runVarMemory();
        new MultiLSMThroughputProcessor(basePathMultiLSM, "btree-static-tuned", "write-static-10-write-10", portions10,
                portionNames, new String[] { "" }).runVarMemory();
        new MultiLSMThroughputProcessor(basePathMultiLSM, "btree-dynamic", "write-btree-write-10", portions10,
                portionNames, Arrays.copyOf(POLICIES, 1)).runVarMemory();
        new MultiLSMThroughputProcessor(basePathMultiLSM, "partition", "write-partition-write-10", portions10,
                portionNames, POLICIES).runVarMemory();

        new MultiLSMThroughputProcessor(basePathMultiLSM, "btree-static-default", "write-static-8-write-10", portions10,
                portionNames, new String[] { "" }).runVarSkew();
        new MultiLSMThroughputProcessor(basePathMultiLSM, "btree-static-tuned", "write-static-10-write-10", portions10,
                portionNames, new String[] { "" }).runVarSkew();
        new MultiLSMThroughputProcessor(basePathMultiLSM, "btree-dynamic", "write-btree-write-10", portions10,
                portionNames, Arrays.copyOf(POLICIES, 1)).runVarSkew();
        new MultiLSMThroughputProcessor(basePathMultiLSM, "partition", "write-partition-write-10", portions10,
                portionNames, POLICIES).runVarSkew();
    }

    public static void main(String[] args) throws IOException {
        mainSK();
        //mainMultiLSM();
    }

}
