/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.uci.asterixdb.storage.experiments.feed.gen;

import java.util.Random;

import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.FeedMode;
import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.UpdateDistribution;

public class KVGenerator extends AbstractRecordGenerator {
    private static final String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    private final Random random = new Random(17);
    private final int recordLength;

    public KVGenerator(FeedMode mode, UpdateDistribution dist, double theta, double updateRatio, long startRange,
            int recordSize) {
        super(mode, dist, theta, updateRatio, startRange);
        recordLength = (recordSize) / 2;
    }

    @Override
    protected String doGetNext(long nextId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"key\":int64(\"");
        sb.append(nextId);
        sb.append("\"),");
        sb.append("\"value\":\"");
        genRandomString(recordLength, sb);
        sb.append("\"}\n");
        return sb.toString();
    }

    protected void genRandomString(int length, StringBuilder sb) {
        while (sb.length() < length) { // length of the random string.
            int index = (int) (random.nextFloat() * SALTCHARS.length());
            sb.append(SALTCHARS.charAt(index));
        }
    }

    public static void main(String[] args) {
        KVGenerator gen = new KVGenerator(FeedMode.Sequential, UpdateDistribution.UNIFORM, 0, 0, 0, 100);
        for (int i = 0; i < 10; i++) {
            System.out.println(gen.getNext());
        }
    }
}
