/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.RefreshResult;

import java.util.Collection;
import java.util.Map;

public class CatalogSnapshot extends AbstractRefCounted {

    private Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles;
    private final long id;


    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        this.id = id;
    }

    public Collection<FileMetadata> getSearchableFiles(String df) {
        return dfGroupedSearchableFiles.get(df);
    }

    @Override
    protected void closeInternal() {
        // notify to file deleter, search, etc
    }
}
