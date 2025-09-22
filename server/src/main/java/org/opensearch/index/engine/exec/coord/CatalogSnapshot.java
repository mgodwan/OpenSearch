/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.RefreshResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CatalogSnapshot extends AbstractRefCounted {

    private Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles = new HashMap<>();
    private final long id;


    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        refreshResult.getRefreshedFiles().forEach((df, files) -> {
            dfGroupedSearchableFiles.put(df.name(), files);
        });
        this.id = id;
    }

    public Iterable<String> dataFormats() {
        return dfGroupedSearchableFiles.keySet();
    }

    public Collection<FileMetadata> getSearchableFiles(String df) {
        return dfGroupedSearchableFiles.get(df);
    }

    public List<Segment> getSegments() {
        return null;
    }

    @Override
    protected void closeInternal() {
        // notify to file deleter, search, etc
    }


    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" +
            "dfGroupedSearchableFiles=" + dfGroupedSearchableFiles +
            ", id=" + id +
            '}';
    }

    public static class Segment {
        private final long generation;
        private final Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles;

        public Segment(Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles, long generation) {
            this.dfGroupedSearchableFiles = dfGroupedSearchableFiles;
            this.generation = generation;
        }

        public Collection<FileMetadata> getSearchableFiles(String df) {
            return dfGroupedSearchableFiles.get(df);
        }

        public long getGeneration() {
            return generation;
        }
    }
}
