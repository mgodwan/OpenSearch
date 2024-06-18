/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import java.io.IOException;

/**
 *
 */
public interface TemplateLoader {

    /**
     *
     * @param template
     * @throws IOException
     */
    void loadTemplate(SystemTemplate template) throws IOException;
}
