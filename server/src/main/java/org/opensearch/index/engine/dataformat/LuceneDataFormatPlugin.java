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

import java.util.Set;

/**
 * Built-in Lucene data format plugin. Registered as the universal fallback at priority 0.
 * Lucene supports all field types and provides full-text search, point ranges, doc values, and stored fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneDataFormatPlugin implements DataFormatPlugin {

    /** The Lucene data format name constant */
    public static final String NAME = DataFormat.LUCENE.name();

    private static final DataFormat LUCENE = new DataFormat(
        NAME,
        Set.of(
            DataFormat.Capability.FULL_TEXT_SEARCH,
            DataFormat.Capability.POINT_RANGE,
            DataFormat.Capability.DOC_VALUES,
            DataFormat.Capability.STORED_FIELDS
        ),
        0 // lowest priority — universal fallback
    );

    @Override
    public DataFormat getDataFormat() {
        return LUCENE;
    }

    @Override
    public boolean supportsField(MappedFieldType fieldType) {
        return true; // Lucene handles all field types
    }
}
