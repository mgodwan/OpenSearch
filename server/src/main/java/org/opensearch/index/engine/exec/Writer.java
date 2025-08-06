/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.Engine;

import java.io.IOException;
import java.util.Optional;

public interface Writer<T extends DataFormat, P extends DocumentInput<?>> {
    Engine.IndexResult addDoc(P d) throws IOException;

    Metadata flush(FlushIn flushIn) throws IOException;

    void sync() throws IOException;

    void close();

    Optional<Metadata> getMetadata();
}
