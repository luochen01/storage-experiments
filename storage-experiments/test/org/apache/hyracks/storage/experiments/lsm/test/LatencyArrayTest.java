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

package org.apache.hyracks.storage.experiments.lsm.test;

import org.junit.Test;

import edu.uci.asterixdb.storage.experiments.lsm.simulator.LatencyArray;

public class LatencyArrayTest {
    @Test
    public void testBasic() {
        LatencyArray array = new LatencyArray(2);
        array.add(0.0099, 10);
        array.add(0.0123, 15);
        array.add(0.0199, 2);
        array.add(1, 5);

        array.forEach((latency, count) -> {
            System.out.println(latency + "\t" + count);
            return null;
        });

    }

}
