/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.DocumentWriterPool;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

    private final DocumentWriterPool pool;
    private final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();

    public CompositeIndexingExecutionEngine(PluginsService pluginsService, Any dataformat) {
        for (DataFormat dataFormat : dataformat.getDataFormats()) {
            DataFormatPlugin plugin = pluginsService.filterPlugins(DataFormatPlugin.class).stream()
                .filter(curr -> curr.getDataFormat().equals(dataFormat))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("dataformat [" + dataFormat + "] is not registered."));
            delegates.add(plugin.indexingEngine());
        }
        this.pool = new DocumentWriterPool(() -> new CompositeDataFormatWriter(this));
    }


    @Override
    public List<String> supportedFieldTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createWriter() throws IOException {
        return pool.fetchWriter();
    }

    @Override
    public RefreshResult refresh() {
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = pool.freeAll();
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                FileMetadata metadata = dataFormatWriter.flush(null);
            }
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
