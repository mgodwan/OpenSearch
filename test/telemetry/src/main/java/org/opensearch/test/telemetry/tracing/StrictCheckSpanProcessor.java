/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.test.telemetry.tracing.validators.AllSpansAreEndedProperly;
import org.opensearch.test.telemetry.tracing.validators.AllSpansHaveUniqueId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Strict check span processor to validate the spans.
 */
public class StrictCheckSpanProcessor implements SpanProcessor {
    /**
     * Base constructor.
     */
    public StrictCheckSpanProcessor() {}

    private static Map<String, MockSpanData> spanMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(Span span) {
        spanMap.put(span.getSpanId(), toMockSpanData(span));
    }

    @Override
    public void onEnd(Span span) {
        MockSpanData spanData = spanMap.get(span.getSpanId());
        // Setting EndEpochTime and HasEnded value to true on completion of span.
        if (spanData != null) {
            spanData.setEndEpochNanos(System.nanoTime());
            spanData.setHasEnded(true);
        }
    }

    /**
     * Return list of mock span data at any point of time.
     */
    public List<MockSpanData> getFinishedSpanItems() {
        return new ArrayList<>(spanMap.values());
    }

    private MockSpanData toMockSpanData(Span span) {
        String parentSpanId = (span.getParentSpan() != null) ? span.getParentSpan().getSpanId() : "";
        MockSpanData spanData = new MockSpanData(
            span.getSpanId(),
            parentSpanId,
            span.getTraceId(),
            System.nanoTime(),
            false,
            span.getSpanName(),
            Thread.currentThread().getStackTrace(),
            (span instanceof MockSpan) ? ((MockSpan) span).getAttributes() : Map.of()
        );
        return spanData;
    }

    private static final Logger logger = LogManager.getLogger(StrictCheckSpanProcessor.class);

    /**
     * Ensures the strict check succeeds for all the spans.
     */
    public static void validateTracingStateOnShutdown() {
        List<MockSpanData> spanData = new ArrayList<>(spanMap.values());
        if (spanData.size() != 0) {
            TelemetryValidators validators = new TelemetryValidators(
                Arrays.asList(new AllSpansAreEndedProperly(), new AllSpansHaveUniqueId())
            );
            try {
                for (MockSpanData spanData1: spanData) {
                    if (!spanData1.isHasEnded()) {
                        @SuppressWarnings("rawtypes")
                        ActionListener listener = ActionListener.globeListeners.get(spanData1.getSpanID());
//                        listener.onFailure(new RuntimeException());
                    }
                }
                validators.validate(spanData, 1);
            } catch (Error e) {
                //spanMap.clear();
                throw e;
            }
        }
    }
}
