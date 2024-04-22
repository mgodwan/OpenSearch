/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.EnumSet;

public class MetadataContextTemplateService {

    private final ClusterService clusterService;
    private final ClusterManagerTaskThrottler.ThrottlingKey putThrottlingKey;

    @Inject
    public MetadataContextTemplateService(
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService,
        AliasValidator aliasValidator,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings,
        NamedXContentRegistry xContentRegistry
    ) {
        this.clusterService = clusterService;
        this.putThrottlingKey = clusterService.registerClusterManagerTask("create_context_template", true);
    }

    public void putContextTemplate(
        final String cause,
        final boolean create,
        final String name,
        final TimeValue masterTimeout,
        final String template,
        final ActionListener<AcknowledgedResponse> listener
    ) throws IOException {
        clusterService.submitStateUpdateTask(
            "create-component-template [" + name + "], cause [" + cause + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return masterTimeout;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return putThrottlingKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return addContextTemplate(currentState, create, name, template);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            }
        );
    }

    private ClusterState addContextTemplate(ClusterState currentState, boolean create, String name, String template) {
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).putCustom("", new Metadata.Custom() {
                    @Override
                    public EnumSet<Metadata.XContentContext> context() {
                        return null;
                    }

                    @Override
                    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
                        return null;
                    }

                    @Override
                    public String getWriteableName() {
                        return null;
                    }

                    @Override
                    public Version getMinimalSupportedVersion() {
                        return null;
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {

                    }

                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                        return null;
                    }
                }))
            .build();
    }

    public static final class ContextTemplate {

    }
}
