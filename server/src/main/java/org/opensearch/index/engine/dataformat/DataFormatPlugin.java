/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Plugin interface for providing custom data format implementations.
 * Plugins implement this to register their data format (e.g., Parquet, Lucene)
 * with the {@link DataFormatRegistry} during node bootstrap.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatPlugin {

    /**
     * Returns the data format with its declared capabilities and priority.
     *
     * @return the data format descriptor
     */
    DataFormat getDataFormat();

    /**
     * Checks if this data format can handle the given field type.
     * Used by the registry to route fields to the appropriate format during indexing.
     *
     * @param fieldType the mapped field type to check
     * @return true if this format supports the field type
     */
    boolean supportsField(MappedFieldType fieldType);
}
