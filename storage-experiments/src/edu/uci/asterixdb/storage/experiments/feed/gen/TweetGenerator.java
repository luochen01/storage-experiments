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
import edu.uci.asterixdb.storage.experiments.feed.gen.DataGenerator.TweetMessage;

public class TweetGenerator implements IRecordGenerator {

    private DataGenerator dataGenerator = null;
    private final IdGenerator idGenerator;
    private final int sidRange;

    private long nextId;
    private long nextSid;

    private final Random random = new Random(17);

    public TweetGenerator(FeedMode mode, UpdateDistribution dist, double theta, double updateRatio, long startRange,
            int sidRange) {
        dataGenerator = new DataGenerator();
        this.idGenerator = IdGenerator.create(dist, theta, startRange, updateRatio, mode == FeedMode.Random);
        this.sidRange = sidRange;
    }

    @Override
    public String getNext() {
        genNextIds();
        TweetMessage msg = dataGenerator.getNext(nextId, nextSid);
        return msg.getAdmEquivalent(null) + "\n";
    }

    private void genNextIds() {
        nextId = idGenerator.next();
        nextSid = random.nextInt(sidRange);
    }

    @Override
    public boolean isNewRecord() {
        return idGenerator.isNewTweet();
    }

    public static void main(String[] args) {
        TweetGenerator gen = new TweetGenerator(FeedMode.Sequential, UpdateDistribution.UNIFORM, 0, 0, 0, 10000);
        for (int i = 0; i < 10; i++) {
            System.out.println(gen.getNext());
        }
    }

}
