package org.opensearch.test.engine;

import org.apache.lucene.index.FilterDirectoryReader;
import org.opensearch.index.engine.EngineBackedIndexer;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.IndexerFactory;

public class MockIndexerFactory implements IndexerFactory {

    private final Class<? extends FilterDirectoryReader> wrapper;

    public MockIndexerFactory(Class<? extends FilterDirectoryReader> wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public Indexer createIndexer(EngineConfig config) {
        return new EngineBackedIndexer(new MockInternalEngine(config, wrapper));
    }
}
