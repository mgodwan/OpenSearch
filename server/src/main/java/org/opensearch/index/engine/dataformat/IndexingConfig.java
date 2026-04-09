package org.opensearch.index.engine.dataformat;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.commit.Committer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.store.Store;

public record IndexingConfig(Store store,
                             IndexSettings indexSettings,
                             MapperService mapperService,
                             Committer committer) {
}
