/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.engine.exec.coord.LuceneVersionConverter;
import org.opensearch.index.engine.exec.coord.SegmentInfosCatalogSnapshot;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LuceneReplicaCommitter implements Committer {

    private volatile Map<String, String> userData;
    private final Store store;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    // Keyed by catalog snapshot generation — survives snapshot cloning at the upload boundary.
    private final Map<Long, DirectoryReader> readers = new ConcurrentHashMap<>();

    Map<Long, DirectoryReader> readers() {
        ensureOpen();
        return readers;
    }

    public LuceneReplicaCommitter(CommitterConfig committerConfig) throws IOException {
        this.store = committerConfig.engineConfig().getStore();
        this.userData = committerConfig.engineConfig().getStore().readLastCommittedSegmentsInfo().getUserData();
    }

    @Override
    public synchronized CommitResult commit(CommitInput commitInput) throws IOException {
        SegmentInfos sis = getSegmentInfos(commitInput.catalogSnapshot());
        sis.setNextWriteGeneration(commitInput.catalogSnapshot().getLastCommitGeneration());
        sis.setUserData(StreamSupport.stream(commitInput.userData().spliterator(), false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)), false);

        if (commitInput.bumpCounter() > 0) {
            sis.counter = store.readLastCommittedSegmentsInfo().counter + commitInput.bumpCounter();
            sis.changed();
        }
        store.commitSegmentInfos(sis);

        SegmentInfos committed = SegmentInfos.readLatestCommit(store.directory());
        assert sis.getGeneration() == committed.getGeneration() : "Committed generation is different from requested generation";
        assert sis.userData.equals(committed.getUserData()) : "Committer user data is different from requested user data";

        // Encode writer's Lucene version as a long — keeps CatalogSnapshot Lucene-type-agnostic.
        long version = LuceneVersionConverter.encode(committed.getCommitLuceneVersion());
        this.userData = committed.userData;
        return new CommitResult(committed.getSegmentsFileName(), committed.getGeneration(), version);
    }

    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        return userData;
    }

    @Override
    public CommitStats getCommitStats() {
        try {
            return new CommitStats(store.readLastCommittedSegmentsInfo());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return null;
    }

    @Override
    public List<CatalogSnapshot> listCommittedSnapshots() throws IOException {
        return List.of();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            store.decRef();
        }
    }

    @Override
    public void deleteCommit(CatalogSnapshot snapshot) throws IOException {
        //no-op
    }

    @Override
    public boolean isCommitManagedFile(String fileName) {
        return fileName.startsWith(IndexFileNames.SEGMENTS) || IndexWriter.WRITE_LOCK_NAME.equals(fileName);
    }

    @Override
    public byte[] serializeToCommitFormat(CatalogSnapshot catalogSnapshot) throws IOException {
        throw new UnsupportedEncodingException("serialization is only for uploads. store committer is for replicas");
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LuceneCommitter is closed");
        }
    }

    private SegmentInfos getSegmentInfos(CatalogSnapshot catalogSnapshot) {
        if (catalogSnapshot instanceof SegmentInfosCatalogSnapshot sics) {
            return sics.getSegmentInfos();
        } else if (catalogSnapshot instanceof DataformatAwareCatalogSnapshot dfacs) {
            assert dfacs.setReplicatingCommitInfo() instanceof SegmentInfos;
            return (SegmentInfos) dfacs.getReplicatingCommitInfo();
        }
        throw new IllegalStateException("Segment infos cannot be obtained from catalog snapshot: " + catalogSnapshot);
    }
}
