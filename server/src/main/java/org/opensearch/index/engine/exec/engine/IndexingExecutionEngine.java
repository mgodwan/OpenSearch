/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.format.DataFormat;

import java.io.IOException;
import java.util.List;

public interface IndexingExecutionEngine<T extends DataFormat> {
    List<String> supportedFieldTypes();

    Writer<? extends DocumentInput<?>> createWriter() throws IOException; // A writer responsible for data format vended by this engine.

    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    DataFormat getDataFormat();
}
