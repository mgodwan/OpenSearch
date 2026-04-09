package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.DataFormatBasedEngine;
import org.opensearch.index.engine.EngineConfig;

public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Override
    public Indexer createIndexer(EngineConfig config) {
        return new DataFormatBasedEngine(config);
    }
}
