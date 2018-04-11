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

import java.io.IOException;
import java.util.Random;

import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

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

    public static void main(String[] args) throws IOException {
        TweetGenerator gen = new TweetGenerator(FeedMode.Sequential, UpdateDistribution.UNIFORM, 0, 0, 0, 1000000);
        ARecordType userType = new ARecordType("userType",
                new String[] { "screen_name", "language", "friends_count", "status_count", "name", "followers_count" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.AINT32,
                        BuiltinType.ASTRING, BuiltinType.AINT32 },
                true);
        ARecordType tweetType = new ARecordType("tweetType",
                new String[] { "id", "sid", "user", "latitude", "longitude", "created_at", "message_text" },
                new IAType[] { BuiltinType.AINT64, BuiltinType.AINT64, userType, BuiltinType.ADOUBLE,
                        BuiltinType.ADOUBLE, BuiltinType.ADATETIME, BuiltinType.ASTRING },
                true);
        ADMDataParser parser = new ADMDataParser(tweetType, false);

        int total = 1000000;
        long begin = System.currentTimeMillis();
        CharArrayRecord record = new CharArrayRecord();
        ArrayBackedValueStorage result = new ArrayBackedValueStorage();
        for (int i = 0; i < total; i++) {
            String tweet = gen.getNext();
            result.reset();
            record.reset();
            //record.set(tweet);
            parser.parse(record, result.getDataOutput());
            if (i % 10000 == 0) {
                System.out.println("Parsed " + i + " records");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Finish parsing in " + (end - begin) + "ms");
    }

}
