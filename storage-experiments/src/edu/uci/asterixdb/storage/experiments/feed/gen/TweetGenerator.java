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

import java.util.HashMap;
import java.util.Map;

import edu.uci.asterixdb.storage.experiments.feed.gen.DataGenerator.TweetMessage;
import edu.uci.asterixdb.storage.experiments.feed.gen.DataGenerator.TweetMessageIterator;

public class TweetGenerator {

    public static final String KEY_UPDATE_RATIO = "update_ratio";
    private static final double DEFAULT_UDPATE_RATIO = 0;
    private TweetMessageIterator tweetIterator = null;
    private DataGenerator dataGenerator = null;

    private final double updateRatio;

    public TweetGenerator(Map<String, String> configuration) {
        dataGenerator = new DataGenerator();
        updateRatio = configuration.get(KEY_UPDATE_RATIO) != null ? Double.valueOf(configuration.get(KEY_UPDATE_RATIO))
                : DEFAULT_UDPATE_RATIO;
        tweetIterator = dataGenerator.new TweetMessageIterator(updateRatio);
    }

    public String getNextTweet() {
        TweetMessage msg = tweetIterator.next();
        return msg.getAdmEquivalent(null) + "\n";
    }

    public static void main(String[] args) {
        Map<String, String> conf = new HashMap<>();
        conf.put(KEY_UPDATE_RATIO, "0.2");

        TweetGenerator gen = new TweetGenerator(conf);
        for (int i = 0; i < 100; i++) {
            System.out.print(gen.getNextTweet());
        }

    }

}
