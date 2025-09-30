/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.engine.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.format.DataFormat;

public interface DataFormatPlugin  {

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine();

    DataFormat getDataFormat();
}
