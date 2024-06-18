/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.applicationtemplates.SystemTemplate;
import org.opensearch.cluster.service.applicationtemplates.SystemTemplateInfo;
import org.opensearch.cluster.service.applicationtemplates.SystemTemplatesService;
import org.opensearch.cluster.service.applicationtemplates.TemplateRepository;
import org.opensearch.cluster.service.applicationtemplates.TemplateRepositoryInfo;
import org.opensearch.common.util.io.Streams;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CoreSystemTemplatesRepository implements TemplateRepository {

    private static final Logger logger = LogManager.getLogger(SystemTemplatesService.class);

    @Override
    public TemplateRepositoryInfo info() {
        return new TemplateRepositoryInfo("_core_", 1);
    }

    @Override
    public List<SystemTemplateInfo> listTemplates() throws IOException {
        List<SystemTemplateInfo> templateInfos = new ArrayList<>();
        try (InputStream is = SystemTemplatesService.class.getResourceAsStream("managed_templates_list.json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            try (XContentParser listParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, out.toString(StandardCharsets.UTF_8))) {
                while (listParser.currentToken() != XContentParser.Token.START_ARRAY) {
                    listParser.nextToken();
                }
                while (listParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    String fileName = listParser.text();
                    SystemTemplateInfo templateInfo = new SystemTemplateInfo();
                    templateInfo.version = 1l;
                    templateInfo.name = fileName;
                    templateInfo.type = "abc_template";
                    templateInfos.add(templateInfo);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("Could not load system templates: ", ex);
        }
        return templateInfos;
    }

    @Override
    public SystemTemplate fetchTemplate(SystemTemplateInfo templateInfo) {
        try (InputStream is = SystemTemplatesService.class.getResourceAsStream(templateInfo.name)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            String template = out.toString(StandardCharsets.UTF_8);
            logger.info(template);
            return new SystemTemplate(template, templateInfo, info());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
