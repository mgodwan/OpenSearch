/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices.context;

import org.opensearch.action.admin.indices.template.contextaware.PutContextTemplateRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestContextTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(List.of(new Route(PUT, "/_context_template/{name}")));
    }

    public static final String ACTION_NAME = "put_context_template";

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String settings = (String) XContentHelper.convertToMap(request.requiredContent(), false, request.getMediaType()).v2()
            .get("settings");
        PutContextTemplateRequest putContextTemplateRequest = new PutContextTemplateRequest()
            .name(request.param("name"))
            .settings(settings);
        logger.info("Request: " + putContextTemplateRequest);
        return channel -> client.admin().indices().putContextTemplate(putContextTemplateRequest, new RestToXContentListener<>(channel));
    }
}
