/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

public class SystemTemplate {
    private final String templateContent;

    private final SystemTemplateInfo templateInfo;

    private final TemplateRepositoryInfo repositoryInfo;

    public SystemTemplate(String templateContent, SystemTemplateInfo templateInfo, TemplateRepositoryInfo repositoryInfo) {
        this.templateContent = templateContent;
        this.templateInfo = templateInfo;
        this.repositoryInfo = repositoryInfo;
    }

    public String templateContent() {
        return templateContent;
    }

    public SystemTemplateInfo templateInfo() {
        return templateInfo;
    }

    public TemplateRepositoryInfo repositoryInfo() {
        return repositoryInfo;
    }
}
