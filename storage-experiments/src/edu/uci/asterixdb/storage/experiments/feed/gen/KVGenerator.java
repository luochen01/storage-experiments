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

public class KVGenerator implements IRecordGenerator {
    private static final String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    private final Random random = new Random(17);

    public KVGenerator() {
    }

    @Override
    public String getRecord(long nextId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"pkey\":");
        sb.append(nextId);
        sb.append(",");
        for (int i = 1; i <= 10; i++) {
            sb.append("\"value");
            sb.append(i);
            sb.append("\":");
            sb.append(random.nextLong());
            if (i < 10) {
                sb.append(",");
            }
        }
        sb.append("}\n");
        return sb.toString();
    }

    public static void main(String[] args) {
        KVGenerator gen = new KVGenerator();
        System.out.println(gen.getRecord(1));
    }

}
