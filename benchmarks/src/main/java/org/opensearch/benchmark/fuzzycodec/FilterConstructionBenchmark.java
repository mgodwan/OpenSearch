/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.fuzzycodec;

import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.*;
import org.opensearch.common.UUIDs;
import org.opensearch.index.codec.fuzzy.*;

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
public class FilterConstructionBenchmark {

    private List<BytesRef> items;

    @Param({ "1000000", "10000000", "50000000" })
    private int numIds;

    @Param({"xor_filter", "bloom_filter"})
    private String setTypeAlias;

    private FuzzySetFactory fuzzySetFactory;
    private String fieldName;

    @Setup
    public void setupIds() {
        this.fieldName = "id";
        this.items = IntStream.range(0, numIds).mapToObj(i -> new BytesRef(UUIDs.base64UUID()))
            .collect(Collectors.toList());
        FuzzySet.SetType setType = FuzzySet.SetType.fromAlias(setTypeAlias);
        FuzzySetParameters parameters = new FuzzySetParameters(0.0511, setType);
        this.fuzzySetFactory = new FuzzySetFactory(Map.of(fieldName, parameters));
    }

    @Benchmark
    public FuzzySet buildFilter() throws IOException  {
        return fuzzySetFactory.createFuzzySet(items.size(), fieldName, () -> items.iterator());
    }
}
