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
import edu.uci.asterixdb.storage.experiments.feed.gen.DataGenerator.TweetMessage;

public class TweetGenerator {

    private DataGenerator dataGenerator = null;
    private final FeedMode mode;
    private final double updateRatio;
    private final long startRange;
    private final long endRange;

    private long nextId;
    private long nextSid;

    private long counter = 0;

    private final Random random = new Random(17);

    public TweetGenerator(FeedMode mode, double updateRatio, long startRange, long endRange) {
        dataGenerator = new DataGenerator();
        this.mode = mode;
        this.updateRatio = updateRatio;
        this.startRange = startRange;
        this.endRange = endRange;
    }

    public String getNextTweet() {
        genNextIds();
        TweetMessage msg = dataGenerator.getNext(nextId, nextSid);
        return msg.getAdmEquivalent(null) + "\n";
    }

    private void genNextIds() {
        if (mode == FeedMode.Sequential) {
            if (random.nextDouble() < updateRatio) {
                //update
                nextId = Math.abs(random.nextLong()) % counter;
            } else {
                nextId = counter++;
            }
            nextSid = nextId;
        } else {
            nextId = Math.abs(random.nextLong() % (endRange - startRange)) + startRange;
            nextSid = Math.abs(random.nextLong()) % (endRange - startRange) + startRange;
        }

    }

    public static void main(String[] args) {
        TweetGenerator gen = new TweetGenerator(FeedMode.Update, 0.0, Long.MIN_VALUE, Long.MAX_VALUE);
        long count = 0;
        long totalSize = 0;
        long begin = System.nanoTime();
        while (true) {
            totalSize += gen.getNextTweet().length() * Character.BYTES;
            count++;
            if (count % 250000 == 0) {
                long end = System.nanoTime();
                System.out.println("Produced " + count + " tweets with total size " + (totalSize / 1024) + " KB"
                        + " in " + (end - begin) / 1000000 + " ms");
                begin = end;
            }
        }
    }

}
