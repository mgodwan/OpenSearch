/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;


public class CuckooFilter implements FuzzySet {

    private final org.opensearch.common.util.CuckooFilter delegatingFilter;
    private MurmurHash3.Hash128 scratchHash = new MurmurHash3.Hash128();
    private static final Random RNG  = new Random(0xFFFFF);

    public CuckooFilter(int maxDocs, double fpp) {
        this.delegatingFilter = new org.opensearch.common.util.CuckooFilter(maxDocs, fpp, RNG);
    }

    CuckooFilter(DataInput in) throws IOException {
        this.delegatingFilter = new org.opensearch.common.util.CuckooFilter(StreamWrapper.fromDataInput(in), RNG);
    }

    @Override
    public SetType setType() {
        return SetType.CUCKOO_FILTER_V1;
    }

    @Override
    public Result contains(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, scratchHash);
        return delegatingFilter.mightContain(hash.h1) ? Result.MAYBE : Result.NO;
    }

    @Override
    public void add(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, scratchHash);
        delegatingFilter.add(hash.h1);
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
        delegatingFilter.writeTo(StreamWrapper.wrap(out));
    }

    @Override
    public long ramBytesUsed() {
        return delegatingFilter.getSizeInBytes();
    }
}
