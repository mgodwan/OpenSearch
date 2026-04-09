package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.EngineBackedIndexer;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;

public class EngineBackedIndexerFactory implements IndexerFactory {

    private final EngineFactory engineFactory;

    public EngineBackedIndexerFactory(EngineFactory engineFactory) {
        this.engineFactory = engineFactory;
    }

    @Override
    public Indexer createIndexer(EngineConfig engineConfig) {
        return new EngineBackedIndexer(engineFactory.newReadWriteEngine(engineConfig));
    }
}
