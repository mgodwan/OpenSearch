/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.commit.CommitBuilder;
import org.opensearch.index.engine.exec.commit.CommitPoint;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class LuceneIEEngine implements IndexingExecutionEngine<DataFormat.LuceneDataFormat>, Committer {

    private InternalEngine internalEngine;
    private IndexWriter indexWriter;

    public LuceneIEEngine(InternalEngine internalEngine) {
        this.internalEngine = internalEngine;
    }

    public LuceneIEEngine() throws IOException {
        Directory d = new NIOFSDirectory(Path.of("/Users/mgodwan/Downloads/poc/lucene/"));
        indexWriter = new IndexWriter(d, new IndexWriterConfig());
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }


    @Override
    public CommitPoint commit(CatalogSnapshot catalogSnapshot) {
        catalogSnapshot.getSearchableFiles("lucene");
        return null;
    }

    @Override
    public Writer<? extends DocumentInput<?>> createWriter() throws IOException {
        if (internalEngine == null) {
            Directory d = new NIOFSDirectory(Path.of("/Users/mgodwan/Downloads/poc/lucene_tmp/" + System.currentTimeMillis()));
            return new LuceneWriter(new IndexWriter(d, new IndexWriterConfig()), false);
        }
        return new LuceneWriter(internalEngine.indexWriter, false);
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    public static class LuceneDocumentInput implements DocumentInput<ParseContext.Document> {

        private final ParseContext.Document doc;
        private final IndexWriter writer;

        public LuceneDocumentInput(ParseContext.Document doc, IndexWriter w) {
            this.doc = doc;
            this.writer = w;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            doc.add(new KeywordFieldMapper.KeywordField("f1", new BytesRef("good_field"), null));
        }

        @Override
        public ParseContext.Document getFinalInput() {
            return doc;
        }

        @Override
        public WriteResult addToWriter() throws IOException {
            writer.addDocument(doc);
            return null;
        }

        @Override
        public void close() throws Exception {
            // no-op, reuse writer
        }
    }

    public static class LuceneWriter implements Writer<LuceneDocumentInput> {

        private IndexWriter writer;
        private boolean shouldClose;

        public LuceneWriter(IndexWriter writer, boolean shouldClose) {
            this.writer = writer;
            this.shouldClose = shouldClose;
        }

        @Override
        public WriteResult addDoc(LuceneDocumentInput d) throws IOException {
            writer.addDocument(d.doc);
            return null;
        }

        @Override
        public FileMetadata flush(FlushIn flushIn) throws IOException {
            writer.forceMerge(1);
            writer.flush();
            writer.commit();
            return null;
        }

        @Override
        public void sync() throws IOException {
            writer.flush();
        }

        public Directory getDirectory() {
            return writer.getDirectory();
        }

        @Override
        public void close() throws IOException {
            if (shouldClose) {
                writer.forceMerge(1);
                writer.flush();
                writer.commit();
                writer.close();
            }
        }

        @Override
        public Optional<FileMetadata> getMetadata() {
            return Optional.empty();
        }

        @Override
        public LuceneDocumentInput newDocumentInput() {
            return new LuceneDocumentInput(new ParseContext.Document(), writer);
        }
    }
}
