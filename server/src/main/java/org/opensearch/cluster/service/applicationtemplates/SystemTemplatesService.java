/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class SystemTemplatesService {

    private final List<SystemTemplatesPlugin> systemTemplatesPluginList;

    private static final Logger logger = LogManager.getLogger(SystemTemplatesService.class);

    public SystemTemplatesService(List<SystemTemplatesPlugin> systemTemplatesPluginList) {
        this.systemTemplatesPluginList = systemTemplatesPluginList;
    }

    public void refreshTemplates() {
        logger.info("Loading templates...");
        for (SystemTemplatesPlugin plugin: systemTemplatesPluginList) {
            try {
                TemplateRepository repository = plugin.loadRepository();
                TemplateRepositoryInfo repositoryInfo = repository.info();
                String repoId = repositoryInfo.id();
                long version = repositoryInfo.version();
                logger.info(repoId + " ... " + version);

                for (SystemTemplateInfo templateInfo : repository.listTemplates()) {
                    try {
                        SystemTemplate template = repository.fetchTemplate(templateInfo);
                        plugin.loaderFor(templateInfo).loadTemplate(template);
                    } catch (Exception ex) {
                        logger.error("", ex);
                    }
                }
            } catch (IOException ex) {
                logger.error("", ex);
            }
        }
    }
}
