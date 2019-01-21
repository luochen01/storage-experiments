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

public class LSMFinalizingPagesEstimator implements ILSMFinalizingPagesEstimator {

    private final double numRecordsPerBTreePage;
    private final double numKeysPerBTreePage;
    private final int numBitsPerKey;
    private final int pageSize;

    public LSMFinalizingPagesEstimator(double numRecordsPerPage, double numKeysPerPage,
            double bloomFilterFalsePositiveRate, int pageSize) {
        this.numRecordsPerBTreePage = numRecordsPerPage;
        this.numKeysPerBTreePage = numKeysPerPage;
        this.pageSize = pageSize;

        //        BloomFilterSpecification spec = BloomCalculations.computeBloomSpec(BloomCalculations.maxBucketsPerElement(1),
        //                bloomFilterFalsePositiveRate);
        //this.numBitsPerKey = spec.getNumBucketsPerElements();
        this.numBitsPerKey = 0;
    }

    @Override
    public double estiamtePages(double records) {
        // btree frontier pages
        //double btreeLeafPages = records / numRecordsPerBTreePage;
        //        int height = (int) Math.ceil(Math.log(btreeLeafPages) / Math.log(numKeysPerBTreePage));
        //
        //        int btreePages = height + 1;
        //
        //        double totalBloomFilterBits = numBitsPerKey * records;
        //        int bloomFilterPages = (int) Math.ceil(totalBloomFilterBits / pageSize / Byte.SIZE) + 1;
        //        return btreePages + bloomFilterPages;
        return 0;
    }

    @Override
    public String toString() {
        return String.format("records/page: %f, keys/page: %f, bits/key: %d, page size: %d", numRecordsPerBTreePage,
                numKeysPerBTreePage, numBitsPerKey, pageSize);
    }

}
