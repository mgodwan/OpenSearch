/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.util.ByteArray;

import java.io.Closeable;
import java.io.IOException;

public class BitSet implements Accountable, Closeable {

    private long underlyingArrayLength = 0L;
    private ByteArray byteArray;

    BitSet(long capacity) {
        underlyingArrayLength = ((capacity - 1L) >> 3) + 1;
        this.byteArray = BigArrays.NON_RECYCLING_INSTANCE.withCircuitBreaking().newByteArray(underlyingArrayLength);
    }

    BitSet(IndexInput in) throws IOException {
        underlyingArrayLength = in.readLong();
        RandomAccessInput input = in.randomAccessSlice(in.getFilePointer(), underlyingArrayLength);
        in.skipBytes(underlyingArrayLength);
        this.byteArray = new ByteArray() {
            @Override
            public byte get(long index) {
                try {
                    return input.readByte(index);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public byte set(long index, byte value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean get(long index, int len, BytesRef ref) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void set(long index, byte[] buf, int offset, int len) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void fill(long fromIndex, long toIndex, byte value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] array() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long size() {
                return underlyingArrayLength;
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() {

            }
        };
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(underlyingArrayLength);
        for (int idx = 0; idx < underlyingArrayLength; idx++) {
            out.writeByte(byteArray.get(idx));
        }
    }

    public long cardinality() {
        long tot = 0;
        for (int i = 0; i < underlyingArrayLength; ++i) {
            tot += Long.bitCount(byteArray.get(i));
        }
        return tot;
    }

    public boolean isSet(long index) {
        long i = index >> 3;
        byte val = byteArray.get(i);
        byte bitmask = (byte)(index & 0x07);
        bitmask = (byte)(1 << bitmask);
        return (val & bitmask) != 0;
    }

    public void set(long index) {
        long wordNum = index >> 3;
        byte bitmask = (byte)(index & 0x07);
        bitmask = (byte)(1 << bitmask);
        byte val = byteArray.get(wordNum);
        byteArray.set(wordNum, (byte)((val | bitmask) & 0xFF));
    }

    @Override
    public long ramBytesUsed() {
        return 128L + byteArray.ramBytesUsed();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(byteArray);
    }

    public static void main(String[] args) throws IOException {
        BitSet bs = new BitSet(1000);
        byte[] b =new byte[8192];
        ByteArrayDataOutput out = new ByteArrayDataOutput(b);
        for (int i = 1; i < 1000; i += 2) {
            bs.set(i);
            if (!bs.isSet(i)) {
                throw new IllegalStateException("Expected to be set " + i);
            }
            if (bs.isSet(i - 1)) {
                throw new IllegalStateException("Expected to not be set " + (i-1));
            }

            if (i < 999 && bs.isSet(i + 1)) {
                throw new IllegalStateException("Expected to not be set " + (i));
            }
        }
        bs.writeTo(out);

        BitSet bs2 = new BitSet(new ByteArrayIndexInput("", b));
        for (int i = 1; i < 1000; i += 2) {
            if (!bs2.isSet(i)) {
                throw new IllegalStateException("Expected to be set");
            }
            if (bs2.isSet(i - 1)) {
                throw new IllegalStateException("Expected to not be set");
            }
        }
    }
}
