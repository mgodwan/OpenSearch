/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

public class TemplateRepositoryInfo {

    private final String id;
    private final long version;

    public TemplateRepositoryInfo(String id, long version) {
        this.id = id;
        this.version = version;
    }

    public String id() {
        return id;
    }

    public long version() {
        return version;
    }
}
