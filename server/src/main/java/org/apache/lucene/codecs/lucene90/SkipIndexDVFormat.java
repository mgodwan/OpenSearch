/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class SkipIndexDVFormat extends DocValuesFormat {

    public SkipIndexDVFormat() {
        super("Lucene90Skip");
    }

    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new DVSkipConsumer(state, DVSkipConsumer.DEFAULT_SKIP_INDEX_INTERVAL_SIZE, "Lucene90SkipDocValuesData", "dvd", "Lucene90SkipDocValuesMetadata", "dvm");
    }

    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new DVSkipProducer(state, "Lucene90SkipDocValuesData", "dvd", "Lucene90SkipDocValuesMetadata", "dvm");
    }
}
