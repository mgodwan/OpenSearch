/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracer wrapped {@link ActionListener}
 * @param <Response> response.
 */
public class TraceableActionListener<Response> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(TraceableActionListener.class);

    private final ActionListener<Response> delegate;
    public final Span span;
    private final Tracer tracer;
    public AtomicBoolean notified = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span     span
     * @param tracer tracer
     */
    private TraceableActionListener(ActionListener<Response> delegate, Span span, Tracer tracer) {
        this.delegate = Objects.requireNonNull(delegate);
        this.span = Objects.requireNonNull(span);
        this.tracer = Objects.requireNonNull(tracer);
    }

    /**
     * Factory method.
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     * @return action listener
     */
    public static <Response> ActionListener<Response> create(ActionListener<Response> delegate, Span span, Tracer tracer) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == true) {
            return new TraceableActionListener<Response>(delegate, span, tracer);
        } else {
            return delegate;
        }
    }

    @Override
    public void onResponse(Response response) {
        logger.info("Closing span in onResponse: " + span.getSpanId() + " --- " + Thread.currentThread());
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.onResponse(response);
        } finally {
            span.endSpan();
        }

    }

    @Override
    public void onFailure(Exception e) {
        logger.info("Closing span in onFailure: " + span.getSpanId() + " --- " + Thread.currentThread());
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            delegate.onFailure(e);
        } finally {
            span.setError(e);
            span.endSpan();
        }

    }
}
