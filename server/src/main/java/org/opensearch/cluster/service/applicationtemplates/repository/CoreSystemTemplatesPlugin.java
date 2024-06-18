/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates.repository;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.applicationtemplates.*;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class CoreSystemTemplatesPlugin extends Plugin implements SystemTemplatesPlugin {

    private ClusterService clusterService;
    private Client client;
    private ThreadPool threadPool;

    private Map<String, TemplateLoader> loaders = new HashMap<>();

    public CoreSystemTemplatesPlugin() {
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService, xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry, indexNameExpressionResolver, repositoriesServiceSupplier);
    }

    @Override
    public TemplateRepository loadRepository() throws IOException {
        return new CoreSystemTemplatesRepository();
    }

    @Override
    public TemplateLoader loaderFor(SystemTemplateInfo templateInfo) {
        return loaders.computeIfAbsent(templateInfo.type, k -> {
            if (templateInfo.type.equals("abc_template")) {
                return new ClusterStateComponentTemplateLoader(client, threadPool, () -> clusterService.state());
            }
            return null;
        });
    }
}
