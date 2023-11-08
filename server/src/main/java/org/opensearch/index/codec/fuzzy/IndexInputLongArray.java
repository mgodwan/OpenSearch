/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.OpenSearchException;
import org.opensearch.common.util.LongArray;

import java.io.IOException;

/**
 * A Long array backed by RandomAccessInput.
 */
class IndexInputLongArray implements LongArray {

    private final RandomAccessInput input;
    private final long size;

    IndexInputLongArray(long size, RandomAccessInput input) {
        this.size = size;
        this.input = input;
    }

    @Override
    public void close() {}

    @Override
    public long size() {
        return size;
    }

    @Override
    public synchronized long get(long index) {
        try {
            return input.readLong(index << 3);
        } catch (IOException ex) {
            throw new OpenSearchException(ex);
        }
    }

    @Override
    public long set(long index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long increment(long index, long inc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(long fromIndex, long toIndex, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(IndexInputLongArray.class);
    }
}
