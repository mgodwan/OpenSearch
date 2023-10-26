/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.XPackedInts;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.hash.MurmurHash3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;


public class CuckooFilter extends AbstractFuzzySet {

    private static final Logger logger = LogManager.getLogger(CuckooFilter.class);

    private volatile org.opensearch.common.util.CuckooFilter delegatingFilter;
    private volatile long seed;

    public CuckooFilter(int maxDocs, double fpp, CheckedSupplier<Iterator<BytesRef>, IOException> fieldIteratorProvider) throws IOException {
        int trials = 0;
        boolean succeeded = false;
        while (++trials < 500) {
            try {
                seed = XORFilter.Hash.randomSeed();
                delegatingFilter = new org.opensearch.common.util.CuckooFilter((long)(maxDocs / 0.9), fpp, new Random(seed));
                addAll(fieldIteratorProvider);
                succeeded = true;
                break;
            } catch (IllegalStateException ex) {
                logger.debug("Retrying filter creation");
            }
        }
        if (!succeeded) {
            throw new IllegalStateException("Failed to crate filters on multiple tries");
        }
    }

    CuckooFilter(DataInput in) throws IOException {
        this.seed = in.readLong();
        this.delegatingFilter = new org.opensearch.common.util.CuckooFilter(StreamWrapper.fromDataInput(in), new Random(seed));
    }

    @Override
    public SetType setType() {
        return SetType.CUCKOO_FILTER_V1;
    }

    @Override
    public Result contains(BytesRef value) {
        return delegatingFilter.mightContain(generateKey(value)) ? Result.MAYBE : Result.NO;
    }

    @Override
    public void add(BytesRef value) {
        boolean added = delegatingFilter.add(generateKey(value));
        if(!added) {
            logger.error("Failed to insert into the cuckoo filter: " + value);
            throw new IllegalStateException("Failed to insert into the cuckoo filter: " + value);
        }
    }

    @Override
    public boolean isSaturated() {
        return false;
    }

    @Override
    public Optional<FuzzySet> maybeDownsize() {
        return Optional.empty();
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(seed);
        delegatingFilter.writeTo(StreamWrapper.wrap(out));
    }

    @Override
    public long ramBytesUsed() {
        return delegatingFilter.getSizeInBytes();
    }

    public static void main(String[] args) throws IOException {
//        CuckooFilter filter = new CuckooFilter(100, 0.2047, () ->List.of(new BytesRef("item1"), new BytesRef("item2"), new BytesRef("item3")).iterator());
//        System.out.println(filter.contains(new BytesRef("item1")));
//        System.out.println(filter.contains(new BytesRef("item2")));
//        System.out.println(filter.contains(new BytesRef("item3")));
//        System.out.println(filter.contains(new BytesRef("item4")));
//        System.out.println(filter.contains(new BytesRef("item5")));
//        byte[] b = new byte[1000000];
//        DataOutput output = new ByteArrayDataOutput(b);
//        filter.writeTo(output);
//
//        for (int i = 0; i < 100; i ++) {
//            System.out.print(b[i] + " ");
//        }
//        System.out.println();
//
//        CuckooFilter filter2 = new CuckooFilter(new ByteArrayDataInput(b));
//        System.out.println(filter2.contains(new BytesRef("item1")));
//        System.out.println(filter2.contains(new BytesRef("item2")));
//        System.out.println(filter2.contains(new BytesRef("item3")));
//        System.out.println(filter2.contains(new BytesRef("item4")));
//        System.out.println(filter2.contains(new BytesRef("item5")));

        XPackedInts.Mutable data = XPackedInts.getMutable(323, 32, PackedInts.COMPACT);
        byte[] b = new byte[100000];
        for (int i = 0; i < 323; i ++) {
            data.set(i, i);
        }
        data.save(new ByteArrayDataOutput(b));
        XPackedInts.Mutable data2 = (XPackedInts.Mutable)XPackedInts.getReader(new ByteArrayDataInput(b));
        for (int i = 0; i < 323; i ++) {
            assert data.get(i) == data2.get(i);
        }
    }
}
