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

import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.FeedMode;
import edu.uci.asterixdb.storage.experiments.feed.FileFeedDriver.UpdateDistribution;

public abstract class AbstractRecordGenerator implements IRecordGenerator {

    private final IdGenerator idGenerator;

    public AbstractRecordGenerator(FeedMode mode, UpdateDistribution dist, double theta, double updateRatio,
            long startRange) {
        this.idGenerator = IdGenerator.create(dist, theta, startRange, updateRatio, mode == FeedMode.Random);
    }

    @Override
    public String getNext() {
        long nextId = idGenerator.next();
        return doGetNext(nextId);
    }

    protected abstract String doGetNext(long nextId);

    @Override
    public boolean isNewRecord() {
        return idGenerator.isNewId();
    }

}
