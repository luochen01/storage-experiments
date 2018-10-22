package edu.uci.asterixdb.storage.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hyracks.storage.am.lsm.common.flowcontrol.simulator.RandomVariable;

public class LSMOperationParser {

    private enum MergePolicy {
        Level,
        Tier
    }

    private final MergePolicy policy = MergePolicy.Tier;

    private final String slowPath = "/Users/luochen/Desktop/test_slow.log";
    private final String fastPath = "/Users/luochen/Desktop/test_fast.log";
    private PrintWriter out = null;

    public static void main(String[] args) throws IOException {
        new LSMOperationParser().run();
    }

    public void run() throws IOException {
        parse(new File(slowPath), policy, "Slow");
        parse(new File(fastPath), policy, "Fast");
    }

    private File getOutputFile(MergePolicy policy, String suffix) {
        String path = "/Users/luochen/Documents/Research/projects/asterixdb/hyracks-fullstack/hyracks/hyracks-tests/hyracks-storage-am-lsm-common-test/src/test/java/org/apache/hyracks/storage/am/lsm/common/test/"
                + policy + "FlowControlSpeedSolver" + suffix + ".java";
        File file = new File(path);
        return file;
    }

    private void parse(File file, MergePolicy policy, String classSuffix) throws IOException {

        StringBuilder sb = new StringBuilder();
        String line = null;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while ((line = reader.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }
        reader.close();
        File outputFile = getOutputFile(policy, classSuffix);
        out = new PrintWriter(outputFile);

        produceHeader(policy, classSuffix);

        String text = sb.toString();
        parseToleratedComponents(text);
        parseNumMemoryComponents(text);
        parseMemroyComponentCapacity(text);
        parseMaxIoSpeed(text);
        parseFlushProcessingTime(text);
        parseFlushFinalizeTime(text);
        int levels = parseMergeProcessingTime(text);
        parseMergeFinalizeSpeeds(text);
        parseMergeResultRatios(text);
        parseFlushInitialUsedCapacities(text);
        parseFlushInitialCurrentUsedCapacities(text);
        parseInitialFlushCapacity(text);
        parseInitialFlushFinalizedPages(text);
        parseInitialFlushSubOperationElapsedTime(text);
        parseInitialComponents(text);
        parseInitialMergedComponents(text);
        parseInitialMergeFinalizedPages(text);
        parseInitialMergeSubOperationElapsedTimes(text);
        parseSubOperationPages(text);
        parseBaseLevelCapacity(text);
        parsePageEstimator(text);
        produceFinal(policy, levels);

        out.close();
        out = null;
        System.out.println("Produced " + outputFile);
    }

    private void produceHeader(MergePolicy policy, String classSuffix) {
        out.println("/*\n" + " * Licensed to the Apache Software Foundation (ASF) under one\n"
                + " * or more contributor license agreements.  See the NOTICE file\n"
                + " * distributed with this work for additional information\n"
                + " * regarding copyright ownership.  The ASF licenses this file\n"
                + " * to you under the Apache License, Version 2.0 (the\n"
                + " * \"License\"); you may not use this file except in compliance\n"
                + " * with the License.  You may obtain a copy of the License at\n" + " *\n"
                + " *   http://www.apache.org/licenses/LICENSE-2.0\n" + " *\n"
                + " * Unless required by applicable law or agreed to in writing,\n"
                + " * software distributed under the License is distributed on an\n"
                + " * \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
                + " * KIND, either express or implied.  See the License for the\n"
                + " * specific language governing permissions and limitations\n" + " * under the License.\n" + " */\n"
                + "\n" + "package org.apache.hyracks.storage.am.lsm.common.test;\n" + "\n"
                + "import org.apache.hyracks.storage.am.lsm.common.flowcontrol.ILSMFinalizingPagesEstimator;\n"
                + "import org.apache.hyracks.storage.am.lsm.common.flowcontrol.LSMFinalizingPagesEstimator;\n"
                + "import org.apache.hyracks.storage.am.lsm.common.flowcontrol.simulator.FlowControlSpeedSolver;\n"
                + "import org.apache.hyracks.storage.am.lsm.common.flowcontrol.simulator.FlowControlSpeedSolver.MergePolicyType;\n"
                + "import org.apache.hyracks.storage.am.lsm.common.flowcontrol.simulator.RandomVariable;\n"
                + "import org.junit.Test;\n");
        out.println(String.format("public class %sFlowControlSpeedSolver%s {", policy, classSuffix));
        out.println("    @Test\n" + "    public void test() {");
    }

    private void parseToleratedComponents(String text) {
        int components = Integer.valueOf(parsePart(text, "tolerated components per level: ", "\n"));
        out.println(String.format("int toleratedComponents = %d;", components));
    }

    private void parseNumMemoryComponents(String text) {
        int components = Integer.valueOf(parsePart(text, "num memory components: ", "\n"));
        out.println(String.format("int numMemoryComponents = %d;", components));
    }

    private void parseMemroyComponentCapacity(String text) {
        RandomVariable capacity = parseRandomVariable(parsePart(text, "memory component capacity: ", "\n"));
        out.println(String.format("RandomVariable memoryComponentCapacity = %s;", toString(capacity)));
    }

    private void parseMaxIoSpeed(String text) {
        double maxIoSpeed = Double.valueOf(parsePart(text, "max io speed: ", "\n"));
        out.println(String.format("double maxIoSpeed = %f;", maxIoSpeed));
    }

    private void parseFlushProcessingTime(String text) {
        String part = parsePart(text, "flush processing times: ", "\n");
        RandomVariable[] variables = parseRandomVariableRow(part);
        out.println(String.format("RandomVariable[] flushProcessingTimes = %s;", toString(variables)));
    }

    private void parseFlushFinalizeTime(String text) {
        String part = parsePart(text, "flush finalize times: ", "\n");
        RandomVariable[] variables = parseRandomVariableRow(part);
        out.println(String.format("RandomVariable[] flushFinalizeTimes = %s;", toString(variables)));
    }

    private int parseMergeProcessingTime(String text) {
        String part = parsePart(text, "merge processing times: \n", "]]\n");
        RandomVariable[][] variables = parseRandomVariables(part);
        out.println(String.format("RandomVariable[][] mergeProcessingTimes = %s;", toString(variables)));
        return variables.length;
    }

    private void parseMergeFinalizeSpeeds(String text) {
        String part = parsePart(text, "merge finalize times: \n", "]]\n");
        RandomVariable[][] variables = parseRandomVariables(part);
        out.println(String.format("RandomVariable[][] mergeFinalizeTimes = %s;", toString(variables)));
    }

    private void parseMergeResultRatios(String text) {
        String part = parsePart(text, "merge component ratios: ", "]]\n");
        RandomVariable[][] variables = parseRandomVariables(part);
        out.println(String.format("RandomVariable[][] componentRatios = %s;", toString(variables)));
    }

    private void parseFlushInitialUsedCapacities(String text) {
        String part = parsePart(text, "inital used memory: ", "\n");
        part = part.replaceAll("\\[", "{");
        part = part.replaceAll("\\]", "}");
        out.println(String.format("double[] initialUsedMemory = %s;", part));
    }

    private void parseFlushInitialCurrentUsedCapacities(String text) {
        String part = parsePart(text, "initial current used memory: ", "\n");
        out.println(String.format("double initialCurrentUsedMemory = %f;", Double.valueOf(part)));
    }

    private void parseInitialFlushCapacity(String text) {
        String part = parsePart(text, "initial flush capacity: ", "\n");
        double memory = Double.parseDouble(part);
        out.println(String.format("double initialFlushCapacity= %f;", memory));
    }

    private void parseInitialFlushFinalizedPages(String text) {
        String part = parsePart(text, "initial flush finalized pages: ", "\n");
        double memory = Double.parseDouble(part);
        out.println(String.format("double initialFlushFinalizedPages = %f;", memory));
    }

    private void parseInitialFlushSubOperationElapsedTime(String text) {
        String part = parsePart(text, "initial flush sub operation elapsed time: ", "\n");
        double memory = Double.parseDouble(part);
        out.println(String.format("double initialFlushSubOperationElapsedTime = %f;", memory));
    }

    private void parseInitialComponents(String text) {
        String part = parsePart(text, "initial merge components: ", "]]\n");
        part += "]]";
        part = part.replaceAll("\\[", "{");
        part = part.replaceAll("\\]", "}");
        out.println(String.format("double[][] initialComponents = %s;", part));
    }

    private void parseInitialMergedComponents(String text) {
        String part = parsePart(text, "initial merged components: ", "\n");
        part = part.replaceAll("\\[", "{");
        part = part.replaceAll("\\]", "}");
        out.println(String.format("double[] initialMergedComponents = %s;", part));
    }

    private void parseInitialMergeFinalizedPages(String text) {
        String part = parsePart(text, "initial merge finalized pages: ", "\n");
        part = part.replaceAll("\\[", "{");
        part = part.replaceAll("\\]", "}");
        out.println(String.format("double[] initialMergeFinalizedPages= %s;", part));
    }

    private void parseInitialMergeSubOperationElapsedTimes(String text) {
        String part = parsePart(text, "initial merge sub operation elapsed times: ", "\n");
        part = part.replaceAll("\\[", "{");
        part = part.replaceAll("\\]", "}");
        out.println(String.format("double[] initialMergeSubOperationElapsedTimes = %s;", part));
    }

    private void parseSubOperationPages(String text) {
        double subOeprationPages = Double.valueOf(parsePart(text, "sub operation pages: ", "\n"));
        out.println(String.format("double subOperationPages = %f;", subOeprationPages));
    }

    private void parseBaseLevelCapacity(String text) {
        double baseLevelCapacity = 0;
        try {
            baseLevelCapacity = Double.valueOf(parsePart(text, "base level capacity: ", "\n"));
        } catch (Exception e) {
            //e.printStackTrace();
        }
        out.println(String.format("double baseLevelCapacity = %f;", baseLevelCapacity));
    }

    private void parsePageEstimator(String text) {
        double recordsPerPage = Double.valueOf(parsePart(text, "records/page: ", ","));
        double keysPerPage = Double.parseDouble(parsePart(text, "keys/page: ", ","));
        double bitsPerKey = Double.parseDouble(parsePart(text, "bits/key: ", ","));
        double pageSize = Double.parseDouble(parsePart(text, "page size: ", "\n"));
        out.println(String.format("double recordsPerPage = %f;", recordsPerPage));
        out.println(String.format(
                "ILSMFinalizingPagesEstimator estimator = new LSMFinalizingPagesEstimator(%f, %f, %f, %d);",
                recordsPerPage, keysPerPage, 0.01, (int) pageSize));
    }

    private void produceFinal(MergePolicy policy, int levels) {
        int sizeRatio = policy == MergePolicy.Level ? 10 : 3;
        out.println(" FlowControlSpeedSolver solver = new FlowControlSpeedSolver(toleratedComponents, " + levels
                + " ,\n" + "                memoryComponentCapacity, numMemoryComponents, " + sizeRatio
                + ", maxIoSpeed, \n" + "                flushProcessingTimes, flushFinalizeTimes,\n"
                + "                mergeProcessingTimes, mergeFinalizeTimes, initialUsedMemory, initialCurrentUsedMemory,\n"
                + "                initialFlushCapacity, initialFlushFinalizedPages,\n"
                + "                initialFlushSubOperationElapsedTime, initialComponents, \n"
                + "                initialMergedComponents, initialMergeFinalizedPages, initialMergeSubOperationElapsedTimes,\n"
                + "                componentRatios, estimator,\n" + "                MergePolicyType."
                + policy.toString().toUpperCase()
                + ", recordsPerPage * subOperationPages, subOperationPages, baseLevelCapacity);");

        out.println("solver.solveMaxSpeedProbSampling();");
        out.println("}");
        out.println("}");

    }

    private RandomVariable[][] parseRandomVariables(String text) {
        text += "]]";
        text = text.substring(text.indexOf('[') + 1, text.lastIndexOf(']'));
        String[] rows = text.split(",\n");
        RandomVariable[][] matrix = new RandomVariable[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            matrix[i] = parseRandomVariableRow(rows[i]);
        }
        return matrix;
    }

    private RandomVariable[] parseRandomVariableRow(String row) {
        row = row.substring(1, row.length() - 1);
        String[] columns = row.split(", ");
        RandomVariable[] variables = new RandomVariable[columns.length];
        for (int i = 0; i < columns.length; i++) {
            variables[i] = parseRandomVariable(columns[i]);
        }
        return variables;

    }

    private RandomVariable parseRandomVariable(String text) {
        if (text.equals("null")) {
            return null;
        }
        String[] parts = text.split("/");
        double mean = Double.valueOf(parts[0]);
        double std = Double.valueOf(parts[1]);
        String[] ranges = parts[2].substring(1, parts[2].length() - 1).split(",");
        double min = Double.valueOf(ranges[0]);
        double max = Double.valueOf(ranges[1]);
        return RandomVariable.of(mean, std, min, max);
    }

    private String toString(RandomVariable v) {
        if (v == null) {
            return "null";
        } else {
            return String.format("RandomVariable.of(%f, %f, %f, %f)", v.mean, v.std, v.min, v.max);
        }
    }

    private String toString(RandomVariable[] variables) {
        if (variables == null) {
            return "null";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < variables.length; i++) {
                sb.append(toString(variables[i]));
                if (i < variables.length - 1) {
                    sb.append(",");
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private String toString(RandomVariable[][] variables) {
        if (variables == null) {
            return "null";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < variables.length; i++) {
                sb.append(toString(variables[i]));
                if (i < variables.length - 1) {
                    sb.append(",");
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private String parsePart(String text, String del, String endDel) {
        int index = text.indexOf(del);
        int endIndex = text.indexOf(endDel, index);
        return text.substring(index + del.length(), endIndex);
    }

}
