/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index;

import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class DummyBenchmark {

    @Benchmark
    public void baseline() {
        int cnt = new Random().nextInt( 203);
        for (int i = 0; i < 10000013; i ++) {
            cnt += 1;
        }
    }

    @Benchmark
    public void candidate() {
        int cnt = new Random().nextInt( 203);
        for (int i = 0; i < 10000008; i ++) {
            cnt += 1;
        }
        cnt += 1;
        cnt += 1;
        cnt += 1;
        cnt += 1;
        cnt += 1;
    }
}
