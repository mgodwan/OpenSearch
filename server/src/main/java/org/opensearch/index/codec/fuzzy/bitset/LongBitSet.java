/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy.bitset;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

public class LongBitSet implements Accountable, Closeable {

    private long underlyingArrayLength = 0L;
    private LongArray longArray;
    public static volatile boolean offHeapEnabled;

    long MOD_DEN = 0x3F;

    public LongBitSet(long capacity) {
        underlyingArrayLength = ((capacity - 1L) >> 6) + 1;
        this.longArray = BigArrays.NON_RECYCLING_INSTANCE.withCircuitBreaking().newLongArray(underlyingArrayLength);
    }

    public LongBitSet(IndexInput in) throws IOException {
        underlyingArrayLength = in.readVLong();
        long streamLength = underlyingArrayLength * 8L;
        if (offHeapEnabled) {
            this.longArray = new IndexInputLongArray(underlyingArrayLength, in.randomAccessSlice(in.getFilePointer(), streamLength));
            in.skipBytes(streamLength);
        } else {
            this.longArray = BigArrays.NON_RECYCLING_INSTANCE.withCircuitBreaking().newLongArray(underlyingArrayLength);
            for (int idx = 0; idx < underlyingArrayLength; idx ++) {
                longArray.set(idx, in.readLong());
            }
        }
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeVLong(underlyingArrayLength);
        for (int idx = 0; idx < underlyingArrayLength; idx ++) {
            out.writeLong(longArray.get(idx));
        }
    }

    public long cardinality() {
        long tot = 0;
        for (int i = 0; i < underlyingArrayLength; ++i) {
            tot += Long.bitCount(longArray.get(i));
        }
        return tot;
    }

    public boolean isSet(long index) {
        long i = index >> 6; // div 64
        long val = longArray.get(i);
        // signed shift will keep a negative index and force an
        // array-index-out-of-bounds-exception, removing the need for an explicit check.
        long bitmask = 1L << index;
        return (val & bitmask) != 0;
    }

    public void set(long index) {
        long wordNum = index >> 6; // div 64
        long bitmask = 1L << index;
        long val = longArray.get(wordNum);
        longArray.set(wordNum, val | bitmask);
    }

    @Override
    public long ramBytesUsed() {
        return 128L + longArray.ramBytesUsed();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(longArray);
    }
}
