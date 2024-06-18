/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.List;

public class TestSystemTemplatesRepositoryPlugin extends Plugin implements SystemTemplatesPlugin {

    @Override
    public TemplateRepository loadRepository() throws IOException {
        return new TemplateRepository() {
            @Override
            public TemplateRepositoryInfo info() {
                return new TemplateRepositoryInfo("core", 1);
            }

            @Override
            public List<SystemTemplateInfo> listTemplates() throws IOException {
                return List.of();
            }

            @Override
            public SystemTemplate fetchTemplate(SystemTemplateInfo template) throws IOException {
                return null;
            }
        };
    }

    @Override
    public TemplateLoader loaderFor(SystemTemplateInfo templateInfo) {
        return null;
    }
}
