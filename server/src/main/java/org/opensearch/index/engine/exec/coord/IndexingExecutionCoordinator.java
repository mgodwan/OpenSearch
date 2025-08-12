/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IndexingExecutionCoordinator {

    private CompositeIndexingExecutionEngine engine;
    private List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private CatalogSnapshot catalogSnapshot;

    public IndexingExecutionCoordinator(MapperService mapperService, EngineConfig engineConfig) {
        refreshListeners = engineConfig.getExternalRefreshListener();
        this.engine = new CompositeIndexingExecutionEngine(null, new Any(List.of(DataFormat.LUCENE)));
    }

    public Engine.IndexResult index(Engine.Index index) throws IOException {
        WriteResult writeResult = engine.createWriter().addDoc(index.documentInput);
        // translog, checkpoint, other checks
        return new Engine.IndexResult(writeResult.version(), writeResult.seqNo(), writeResult.term(), writeResult.success());
    }


    public synchronized void refresh(String source) throws EngineException, IOException {
        refreshListeners.forEach(ref -> {
            try {
                ref.beforeRefresh();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        // build catalog snapshot, will revisit the engine's interface to do this work.
        catalogSnapshot.incRef();

        refreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        catalogSnapshot.incRef(); // this should be package-private
        return new ReleasableRef<CatalogSnapshot>(catalogSnapshot) {
            @Override
            public void close() throws Exception {
                catalogSnapshot.decRef(); // this should be package-private
            }
        };
    }



    public static abstract class ReleasableRef<T> implements AutoCloseable {
        private T t;

        public ReleasableRef(T t) {
            this.t = t;
        }

        public T getRef() {
            return t;
        }
    }

}
