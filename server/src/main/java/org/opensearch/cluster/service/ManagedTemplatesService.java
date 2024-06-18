/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.common.util.io.Streams;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ManagedTemplatesService {

    private Supplier<Client> client;

    private static final Logger logger = LogManager.getLogger(ManagedTemplatesService.class);

    private Set<String> listProcessed;

    public ManagedTemplatesService(Supplier<Client> clientProvider, ThreadPool threadPool) {
        this.client = clientProvider;
    }

    void loadTemplates(Map<String, ComponentTemplate> metadata)   {
        try (InputStream is = ManagedTemplatesService.class.getResourceAsStream("managed_templates_list.json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            try (XContentParser listParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, out.toString(StandardCharsets.UTF_8))) {
                while (listParser.currentToken() != XContentParser.Token.START_ARRAY) {
                    listParser.nextToken();
                }
                while (listParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    String fileName = listParser.text();
                    logger.info("Loading template: " + fileName);
                    loadTemplate(fileName, metadata);
                    listParser.nextToken();
                }
            }

        } catch (Exception ex) {
            throw new RuntimeException("Could not load managed templates", ex);
        }
    }

    private void loadTemplate(String fileName, Map<String, ComponentTemplate> existingMetadata) throws IOException {
        try (InputStream is = ManagedTemplatesService.class.getResourceAsStream(fileName)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            String name = fileName.split("\\.")[0];
            String template = out.toString(StandardCharsets.UTF_8);
            logger.info(template);
            XContentParser contentParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, template.getBytes(StandardCharsets.UTF_8));
            try {
                PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(name).componentTemplate(ComponentTemplate.parse(contentParser));
                if (!existingMetadata.containsKey(name) || existingMetadata.get(name).version() < request.componentTemplate().version()) {
                    client.get().admin().indices().execute(PutComponentTemplateAction.INSTANCE, request, new ActionListener<AcknowledgedResponse>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            logger.info("success");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("failed: " + e);
                        }
                    });
                } else {
                    logger.debug("Short circuited reload for same template version: " + name);
                }
            } catch (Exception ex) {
                logger.error("failed to load template: ", ex);
                throw new RuntimeException(ex);
            }
        }
    }
}
