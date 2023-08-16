/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.time;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.openjdk.jmh.annotations.*;
import org.opensearch.index.codec.CustomSourceFieldCodec;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class SourceTransformationBenchmark {

    private String s = "{\"total_amount\": 15.0, \"improvement_surcharge\": 0.0, \"pickup_location\": [-73.86389923095703, 40.89543914794922], \"pickup_datetime\": \"2015-01-01 00:35:03\", \"trip_type\": \"2\", \"dropoff_datetime\": \"2015-01-01 00:35:08\", \"rate_code_id\": \"5\", \"tolls_amount\": 0.0, \"dropoff_location\": [-73.86187744140625, 40.894779205322266], \"passenger_count\": 1, \"fare_amount\": 15.0, \"extra\": 0.0, \"trip_distance\": 0.13, \"tip_amount\": 0.0, \"store_and_fwd_flag\": \"N\", \"payment_type\": \"1\", \"mta_tax\": 0.0, \"vendor_id\": \"2\"}";
    private byte[] arr;
    private CustomSourceFieldCodec codec;
    private ByteBuffersDataInput input;

    @Setup
    public void setup() throws Exception {
        arr = s.getBytes("UTF-8");
        codec = new CustomSourceFieldCodec();
        input = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(arr)));
    }

    @Benchmark
    public void convertToOrdinals() throws Exception {
        arr = s.getBytes("UTF-8");
        input = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(arr)));
        codec.transferWithOrdinalization(input);
    }

//    @Benchmark
//    public void transferOnly() throws Exception {
//        arr = s.getBytes("UTF-8");
//        input = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(arr)));
//        codec.transferAsIs(input);
//    }
}
