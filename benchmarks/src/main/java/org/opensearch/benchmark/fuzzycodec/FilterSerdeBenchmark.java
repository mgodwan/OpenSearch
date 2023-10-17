/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.fuzzycodec;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.*;
import org.opensearch.common.UUIDs;
import org.opensearch.index.codec.fuzzy.FuzzySet;
import org.opensearch.index.codec.fuzzy.FuzzySetFactory;
import org.opensearch.index.codec.fuzzy.FuzzySetParameters;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3, time = 60, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FilterSerdeBenchmark {

    @Param({"xor_filter", "bloom_filter"})
    private String setTypeAlias;

    @Param({"1000000", "10000000", "50000000"})
    private int numItems;

    private FuzzySet fuzzySet;
    private byte[] serializedFilter;

    @Setup
    public void setupFilter() throws IOException {
        String fieldName = "id";
        List<BytesRef> items = IntStream.range(0, numItems).mapToObj(i -> new BytesRef(UUIDs.base64UUID()))
            .collect(Collectors.toList());
        FuzzySet.SetType setType = FuzzySet.SetType.fromAlias(setTypeAlias);
        FuzzySetParameters parameters = new FuzzySetParameters(0.0511, setType);
        fuzzySet = new FuzzySetFactory(Map.of(fieldName, parameters))
            .createFuzzySet(numItems, fieldName, () -> items.iterator());
        serializedFilter = new byte[(int) fuzzySet.ramBytesUsed() + 8192];
        ByteArrayDataOutput dop = new ByteArrayDataOutput(serializedFilter);
        dop.writeString(setType.getSetName());
        fuzzySet.writeTo(dop);
    }

    @Benchmark
    public void serialize() throws IOException {
        ByteArrayDataOutput dop = new ByteArrayDataOutput();
        fuzzySet.writeTo(dop);
    }

    @Benchmark
    public void deserialize() throws IOException {
        //FuzzySetFactory.deserializeFuzzySet(new ByteArrayDataInput(serializedFilter));
    }
}
