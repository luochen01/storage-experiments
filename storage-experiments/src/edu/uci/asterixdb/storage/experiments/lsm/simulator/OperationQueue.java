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

import java.util.Arrays;

public class OperationQueue {

    private SubOperation[] queue;

    private int size = 0;

    private static final int DEFAULT_INITIAL_CAPACITY = 10;

    public OperationQueue() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public OperationQueue(int initialCapacity) {
        if (initialCapacity < 1)
            throw new IllegalArgumentException();
        this.queue = new SubOperation[initialCapacity];
    }

    private void grow(int minCapacity) {
        int oldCapacity = queue.length;
        int newCapacity = oldCapacity + ((oldCapacity < 64) ? (oldCapacity + 2) : (oldCapacity >> 1));
        queue = Arrays.copyOf(queue, newCapacity);
    }

    public boolean add(SubOperation e) {
        assert e.active;
        assert e.remainingTime > 0;
        assert sanityCheck(e);
        int i = size;
        if (i >= queue.length)
            grow(i + 1);
        size = i + 1;
        if (i == 0)
            queue[0] = e;
        else
            siftUp(i, e);
        return true;
    }

    private boolean sanityCheck(SubOperation e) {
        for (int i = 0; i < size; i++) {
            assert queue[i] != e;
        }
        return true;
    }

    public int size() {
        return size;
    }

    public void replaceTop(SubOperation e) {
        assert size > 0;
        queue[0] = e;
        siftDown(0, e);
    }

    public void removeTop() {
        int s = --size;
        SubOperation moved = queue[s];
        queue[s] = null;
        siftDown(0, moved);
        if (queue[0] == moved) {
            siftUp(0, moved);
        }
        assert size > 0;
    }

    public SubOperation get(int i) {
        return queue[i];
    }

    public SubOperation peek() {
        return (size == 0) ? null : queue[0];
    }

    public void clear() {
        for (int i = 0; i < size; i++) {
            queue[i] = null;
        }
        size = 0;
    }

    public SubOperation poll() {
        if (size == 0)
            return null;
        int s = --size;
        SubOperation result = queue[0];
        SubOperation x = queue[s];
        queue[s] = null;
        if (s != 0) {
            siftDown(0, x);
        }
        return result;
    }

    private void siftUp(int k, SubOperation x) {
        SubOperation key = x;
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            SubOperation e = queue[parent];
            if (key.compareTo(e) >= 0)
                break;
            queue[k] = e;
            k = parent;
        }
        queue[k] = key;
    }

    private void siftDown(int k, SubOperation x) {
        SubOperation key = x;
        int half = size >>> 1; // loop while a non-leaf
        while (k < half) {
            int child = (k << 1) + 1; // assume left child is least
            SubOperation c = queue[child];
            int right = child + 1;
            if (right < size && c.compareTo(queue[right]) > 0)
                c = queue[child = right];
            if (key.compareTo(c) <= 0)
                break;
            queue[k] = c;
            k = child;
        }
        queue[k] = key;
    }

}
