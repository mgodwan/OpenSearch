/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class LuceneIEEngine implements IndexingExecutionEngine<LuceneDataFormat> {

    @Override
    public List<String> supportedFieldTypes() {
        return List.of(BooleanFieldMapper.CONTENT_TYPE,  BinaryFieldMapper.CONTENT_TYPE,  DateFieldMapper.CONTENT_TYPE,
            NumberFieldMapper.NumberType.INTEGER.typeName(), NumberFieldMapper.NumberType.DOUBLE.typeName());
    }

    @Override
    public Writer<LuceneDataFormat, LuceneDocumentInput> createWriter() throws IOException {
        return new Writer<>() {

            @Override
            public Engine.IndexResult addDoc(LuceneDocumentInput d) throws IOException {
                IndexWriter writer = new IndexWriter(FSDirectory.open(Path.of("alsad")), new IndexWriterConfig());
                writer.addDocument(d.getFinalInput());
                return null;
            }

            @Override
            public Metadata flush(FlushIn flushIn) throws IOException {
                return null;
            }

            public void sync() throws IOException {
                 // no-op for this engine as we will end up having committed in flush step only.
            }

            @Override
            public void close() {

            }

            @Override
            public Optional<Metadata> getMetadata() {
                return Optional.empty();
            }
        };
    }


}
