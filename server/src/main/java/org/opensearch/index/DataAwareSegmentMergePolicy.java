/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper around {@link TieredMergePolicy} which doesn't respect
 * {@link TieredMergePolicy#setMaxMergedSegmentMB(double)} on forced merges, but DOES respect it on only_expunge_deletes.
 * See https://issues.apache.org/jira/browse/LUCENE-7976.
 *
 * @opensearch.internal
 */
public class DataAwareSegmentMergePolicy extends TieredMergePolicy {
    /**
     * Creates a new filter merge policy instance wrapping another.
     *
     */
    public DataAwareSegmentMergePolicy() {
        super();
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        try {
            final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
            MergeSpecification spec = null;
            final Map<String, List<SegmentCommitInfo>> commitInfos = new HashMap<>();
            for (SegmentCommitInfo si : infos) {
                if (merging.contains(si)) {
                    continue;
                }
                commitInfos.computeIfAbsent(si.info.getAttribute("bucket"), k -> new ArrayList<>()).add(si);
            }
            for (String bucket: commitInfos.keySet()) {
                if (commitInfos.get(bucket).size() > 1) {
                    if (spec == null) {
                        spec = new MergeSpecification();
                    }
                    spec.add(new OneMerge(commitInfos.get(bucket)));
                }
            }
            return spec;
        } catch (Exception ex) {
            return super.findMerges(mergeTrigger, infos, mergeContext);
        }
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
        return findMerges(null, infos, mergeContext);
    }
}
