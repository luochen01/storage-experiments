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
package edu.uci.asterixdb.storage.experiments.lsm.simulator;

public class SpeedProvider implements ISpeedProvider {

    private final RandomVariable[] processingSpeeds;
    private final RandomVariable[] finalizeSpeeds;

    public SpeedProvider(RandomVariable[] processingSpeeds, RandomVariable[] finalizeSpeeds) {
        this.processingSpeeds = processingSpeeds;
        this.finalizeSpeeds = finalizeSpeeds;
    }

    @Override
    public double getProcessingSpeed(int ioOperations) {
        RandomVariable stat = processingSpeeds[ioOperations];
        return DoubleUtil.nextGaussian(stat);
    }

    @Override
    public double getFinalizeSpeed(int ioOperations) {
        RandomVariable stat = finalizeSpeeds[ioOperations];
        return DoubleUtil.nextGaussian(stat);
    }

}
