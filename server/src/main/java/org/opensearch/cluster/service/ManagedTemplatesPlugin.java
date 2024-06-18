/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.cluster.metadata.ComponentTemplate;

import java.util.List;
import java.util.Map;

public interface ManagedTemplatesPlugin {

    List<String> listTemplates();

    ComponentTemplate loadTemplate(String resourceName, Map<String, ComponentTemplate> existingMetadata);
}
