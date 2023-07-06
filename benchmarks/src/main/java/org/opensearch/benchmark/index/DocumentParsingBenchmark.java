/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index;

import com.fasterxml.jackson.core.io.doubleparser.FastDoubleParser;
import org.openjdk.jmh.annotations.*;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.common.time.FastDTParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.DateFieldMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class DocumentParsingBenchmark {

    private String val = Double.toString(new Random().nextDouble());

    private static DateFormatter STRICT_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.ROOT);
    //private static DateFormatter FALLBACK_FORMATTER = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");

    @Benchmark
    public void candidate() throws Exception {
        //STRICT_FORMATTER.parse(nowEpoch);
        DateFormatters.HTTP_LOGS_FORMAT_PARSER.parse("2022-04-05 22:00:12");
    }

    @Benchmark
    public void baseline() throws Exception {
        STRICT_FORMATTER.parse("2022-04-05 22:00:12");
    }
}
