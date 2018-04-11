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

public class TweetGenerator extends AbstractRecordGenerator {

    private DataGenerator dataGenerator = null;
    private final int sidRange;

    private final Random random = new Random(17);

    public TweetGenerator(FeedMode mode, UpdateDistribution dist, double theta, double updateRatio, long startRange,
            int sidRange) {
        super(mode, dist, theta, updateRatio, startRange);
        dataGenerator = new DataGenerator();
        this.sidRange = sidRange;
    }

    @Override
    protected String doGetNext(long nextId) {
        TweetMessage msg = dataGenerator.getNext(nextId, random.nextInt(sidRange));
        return msg.getAdmEquivalent(null) + "\n";
    }

}
