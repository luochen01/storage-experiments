package edu.uci.asterixdb.storage.experiments.memory;

import static edu.uci.asterixdb.storage.experiments.memory.LSMMemoryUtils.memories;

import java.io.IOException;

public class PatternProcessor {

    private final String basePath = "/Users/luochen/Documents/Research/experiments/results/memory/write/single-latency";

    //private final String basePath = "/tmp";

    private final int skip = 600;

    private static final String[] impls = { "btree", "partition", "full-index", "full-data" };
    private static final String workload = "write";

    public PatternProcessor() {
    }

    public void run() throws IOException {
        double[][] results = new double[impls.length][];
        for (int i = 0; i < impls.length; i++) {
            results[i] = runWorkload(impls[i], workload);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("memory");
        sb.append("\t");
        for (String impl : impls) {
            sb.append(impl);
            sb.append("\t");
        }
        System.out.println(sb.toString());

        for (int i = 0; i < memories.length; i++) {
            sb = new StringBuilder();
            sb.append(memories[i]);
            sb.append("\t");
            for (int j = 0; j < impls.length; j++) {
                sb.append(results[j][i]);
                sb.append("\t");
            }
            System.out.println(sb.toString());
        }
    }

    private double[] runWorkload(String impl, String workload) throws IOException {
        double[] results = new double[memories.length];
        for (int i = 0; i < memories.length; i++) {
            results[i] = LSMMemoryUtils.getLogValue(LSMMemoryUtils.getLogFile(basePath, impl, workload, memories[i]),
                    "[Intended-UPDATE], 99thPercentileLatency(us), ") / 1000;
        }
        return results;
    }

    public static void main(String[] args) throws IOException {
        new PatternProcessor().run();
    }

}
