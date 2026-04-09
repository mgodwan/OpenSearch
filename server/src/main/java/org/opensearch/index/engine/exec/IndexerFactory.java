package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineConfig;

import java.util.function.Supplier;

@ExperimentalApi
public interface IndexerFactory {

    Indexer createIndexer(EngineConfig config);
}
