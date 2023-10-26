/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.fuzzycodec;

import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.UUIDs;
import org.opensearch.index.codec.fuzzy.BloomFilter;
import org.opensearch.index.codec.fuzzy.FuzzySet;
import org.opensearch.index.codec.fuzzy.FuzzySetFactory;
import org.opensearch.index.codec.fuzzy.FuzzySetParameters;
import org.opensearch.index.codec.fuzzy.bitset.LongBitSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3, time = 30, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FilterLookupBenchmark {

    @Param({"bloom_filter"})
    private String setTypeAlias;

    @Param({"50000000", "1000000"})
    private int numItems;

    @Param({"1000000"})
    private int searchKeyCount;

    @Param ({"true", "false"})
    private boolean offheapEnabled;

    private FuzzySet fuzzySet;
    private List<BytesRef> items;
    private Random random = new Random();

    @Setup
    public void setupFilter() throws IOException {
        String fieldName = "id";
        items = IntStream.range(0, numItems).mapToObj(i -> new BytesRef(UUIDs.base64UUID()))
            .collect(Collectors.toList());
        FuzzySet.SetType setType = FuzzySet.SetType.fromAlias(setTypeAlias);
        FuzzySetParameters parameters = new FuzzySetParameters(0.0511, setType);
        fuzzySet = new FuzzySetFactory(Map.of(fieldName, parameters))
            .createFuzzySet(numItems, fieldName, () -> items.iterator());
        LongBitSet.offHeapEnabled = offheapEnabled;
        fuzzySet =
    }

    @Benchmark
    public void contains_withExistingKeys(Blackhole blackhole) throws IOException {
        for (int i = 0; i < searchKeyCount; i ++) {
            blackhole.consume(fuzzySet.contains(items.get(random.nextInt(items.size()))) == FuzzySet.Result.MAYBE);
        }
    }

    @Benchmark
    public void contains_withRandomKeys(Blackhole blackhole) throws IOException {
        for (int i = 0; i < searchKeyCount; i ++) {
            blackhole.consume(fuzzySet.contains(new BytesRef(UUIDs.base64UUID())));
        }
    }
}
