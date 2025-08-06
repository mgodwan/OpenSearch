/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.List;

public interface IndexingExecutionEngine<T extends DataFormat> {
    List<String> supportedFieldTypes();

    Writer<T, ? extends DocumentInput<?>> createWriter() throws IOException; // A writer responsible for data format vended by this engine.
}
