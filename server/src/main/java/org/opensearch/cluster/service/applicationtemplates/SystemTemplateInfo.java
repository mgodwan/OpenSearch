/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

public class SystemTemplateInfo {

    public long version;
    public String type;
    public String name;

    public String fullyQualifiedName() {
        return name + "_" + version;
    }
}
