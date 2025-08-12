/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CompositeDataFormatWriter implements Writer<CompositeDataFormatWriter.CompositeDocumentInput> {

    List<Writer<? extends DocumentInput>> writers;

    public CompositeDataFormatWriter(CompositeIndexingExecutionEngine engine) {

    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput d) throws IOException {
        for (DocumentInput docInput: d.getFinalInput()) {
            docInput.getWriter().addDoc(docInput);
        }
        return null;
    }

    @Override
    public FileMetadata flush(FlushIn flushIn) throws IOException {
        return null;
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() {

    }

    @Override
    public Optional<FileMetadata> getMetadata() {
        return Optional.empty();
    }

    @Override
    public CompositeDocumentInput newDocumentInput() {
        List<DocumentInput<?>> documentInputs = new ArrayList<>();
        return new CompositeDocumentInput(writers.stream().map(Writer::newDocumentInput).collect(Collectors.toList()), this);
    }

    public static class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {
        List<? extends DocumentInput<?>> inputs;
        CompositeDataFormatWriter writer;

        public CompositeDocumentInput(List<? extends DocumentInput<?>> inputs, CompositeDataFormatWriter writer) {
            this.inputs = inputs;
            this.writer = writer;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            for (DocumentInput<?> input : inputs) {
                input.addField(fieldType, value);
            }
        }

        @Override
        public List<? extends DocumentInput<?>> getFinalInput() {
            return null;
        }

        @Override
        public CompositeDataFormatWriter getWriter() {
            return writer;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
