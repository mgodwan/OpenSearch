/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.template.contextaware;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.ContextTemplateMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.rest.action.admin.indices.context.RestContextTemplateAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportPutContextTemplateAction extends TransportClusterManagerNodeAction<PutContextTemplateRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutContextTemplateAction.class);

    private final MetadataIndexTemplateService indexTemplateService;
    private final ClusterManagerTaskThrottler.ThrottlingKey createContextTemplateKey;


    @Inject
    public TransportPutContextTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutContextTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutContextTemplateRequest::new,
            indexNameExpressionResolver
        );
        this.indexTemplateService = indexTemplateService;
        createContextTemplateKey = clusterService.registerClusterManagerTask(RestContextTemplateAction.ACTION_NAME, true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutContextTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        final PutContextTemplateRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "create-context-template [" + request.name() + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return request.clusterManagerNodeTimeout();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return createContextTemplateKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ContextTemplateMetadata prev = ((ContextTemplateMetadata) currentState.metadata().custom(ContextTemplateMetadata.TYPE));
                    Map<String, ContextTemplateMetadata.ContextTemplate> contextTemplates = new HashMap<>();
                    if (prev != null) {
                        contextTemplates = new HashMap<>(prev.getContextTemplates());
                    }
                    contextTemplates.put(request.name(), new ContextTemplateMetadata.ContextTemplate(request.name(), request.settings()));
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata())
                            .putCustom(ContextTemplateMetadata.TYPE, new ContextTemplateMetadata(contextTemplates))
                            .build())
                        .build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            }
        );
    }
}
