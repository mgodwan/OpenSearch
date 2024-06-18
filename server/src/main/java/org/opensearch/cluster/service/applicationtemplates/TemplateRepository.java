/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface TemplateRepository {

    /**
     *
     * @return
     */
    TemplateRepositoryInfo info();

    /**
     *
     * @return
     */
    List<SystemTemplateInfo> listTemplates() throws IOException;

    /**
     *
     * @param template
     * @return
     */
    SystemTemplate fetchTemplate(SystemTemplateInfo template) throws IOException;
}
