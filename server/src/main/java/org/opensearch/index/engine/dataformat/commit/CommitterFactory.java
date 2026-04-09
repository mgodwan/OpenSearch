package org.opensearch.index.engine.dataformat.commit;

import org.opensearch.index.store.Store;

public interface CommitterFactory {
    Committer getCommitter(Store store);
}
