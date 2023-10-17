/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy.bitset;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;

import java.io.IOException;

public class LongBitSet implements Accountable {

    private long cardinality = 0;
    private LongArray longArray;

    public LongBitSet(long capacity) {
        this.longArray = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(capacity / 8 + (capacity % 8 == 0 ? 0 : 1));
    }

    public LongBitSet(IndexInput in) throws IOException {
        long capacity = in.readVLong();
        this.longArray = new IndexInputLongArray(capacity, in.randomAccessSlice(in.getFilePointer(), capacity));
    }

    public long cardinality() {
        return cardinality;
    }

    public void set(long idx) {
        assert idx >= 0;
        long backingArrayIndex = idx >> 8;
        long val = longArray.get(backingArrayIndex);
        if ((val & (1 << (idx & 7))) == 0)  cardinality ++;
        val = val | (1 << (idx & 0x07));
        longArray.set(backingArrayIndex, val);
    }

    public boolean isSet(long idx) {
        assert idx >= 0;
        long backingArrayIndex = idx >> 8;
        long val = longArray.get(backingArrayIndex);
        return (val & (1 << (idx & 7))) != 0;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeVLong(longArray.size());
        for (int idx = 0; idx < longArray.size(); idx ++) {
            out.writeVLong(longArray.get(idx));
        }
    }

    @Override
    public long ramBytesUsed() {
        return 128L + longArray.ramBytesUsed();
    }
}
