/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FuzzySetFactory {

    private Map<String, FuzzySetParameters> setTypeForField;

    public FuzzySetFactory(Map<String, FuzzySetParameters> setTypeForField) {
        this.setTypeForField = new ConcurrentHashMap<>(setTypeForField);
    }

    // Only to be used by default constructor for Postings format, needed for Lucene Named SPI
    FuzzySetFactory() {
    }

//
//    // Uncomment when dynamic setting is wired in
//    public void updateMaxFalsePositiveProbability(String fieldName, double maxFalsePositiveProbability) {
//        setTypeForField.compute(fieldName, (k, v) -> new FuzzySetParameters(maxFalsePositiveProbability, v.getSetType()));
//    }

    public FuzzySet createFuzzySet(int maxDocs, String fieldName, CheckedSupplier<Iterator<BytesRef>, IOException> iteratorProvider) throws IOException {
        FuzzySetParameters params = setTypeForField.get(fieldName);
        if (params == null) {
            throw new IllegalArgumentException("No fuzzy set defined for field: " + fieldName);
        }
        switch (params.getSetType()) {
            case BLOOM_FILTER_V1:
                return new BloomFilter(maxDocs, params.getFalsePositiveProbability(), iteratorProvider);
            case CUCKOO_FILTER_V1:
                return new CuckooFilter(maxDocs, params.getFalsePositiveProbability(), iteratorProvider);
            case XOR_FILTER_V1:
                return new XORFilter(iteratorProvider);
            default:
                throw new IllegalArgumentException("No Implementation for set type: " + params.getSetType());
        }
    }

    public static FuzzySet deserializeFuzzySet(DataInput in) throws IOException {
        FuzzySet.SetType setType = FuzzySet.SetType.from(in.readString());
        return setType.getDeserializer().apply(in);
    }
}
