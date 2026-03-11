/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry for data format plugins, built once during node bootstrap.
 * Provides lookup by name and field-to-format routing based on declared capabilities and priority.
 * <p>
 * This follows the same immutable registry pattern as {@link org.opensearch.indices.mapper.MapperRegistry}.
 * Plugins register their {@link DataFormatPlugin} implementations via
 * {@link org.opensearch.plugins.PluginsService}, and the registry is constructed during node startup.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DataFormatRegistry {

    private final Map<String, DataFormatPlugin> formats;
    private final List<DataFormatPlugin> priorityOrder;

    /**
     * Constructs the registry from a list of data format plugins.
     * Plugins are validated for uniqueness and sorted by priority (highest first) for field routing.
     *
     * @param plugins the list of data format plugins to register
     * @throws IllegalArgumentException if duplicate format names are detected
     */
    public DataFormatRegistry(List<DataFormatPlugin> plugins) {
        Map<String, DataFormatPlugin> map = new LinkedHashMap<>();
        for (DataFormatPlugin plugin : plugins) {
            String name = plugin.getDataFormat().name();
            if (map.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate data format registered: [" + name + "]");
            }
            map.put(name, plugin);
        }
        this.formats = Collections.unmodifiableMap(map);
        this.priorityOrder = plugins.stream()
            .sorted(Comparator.comparingInt((DataFormatPlugin p) -> p.getDataFormat().priority()).reversed())
            .toList();
    }

    /**
     * Looks up a plugin by format name. Used during deserialization of Segment/FileMetadata.
     *
     * @param name the data format name
     * @return the plugin, or null if not registered
     */
    public DataFormatPlugin getPlugin(String name) {
        return formats.get(name);
    }

    /**
     * Looks up a DataFormat by name.
     *
     * @param name the data format name
     * @return the DataFormat, or null if not registered
     */
    public DataFormat getFormat(String name) {
        DataFormatPlugin plugin = formats.get(name);
        return plugin != null ? plugin.getDataFormat() : null;
    }

    /**
     * Returns all registered plugins.
     *
     * @return unmodifiable collection of plugins
     */
    public Collection<DataFormatPlugin> getPlugins() {
        return formats.values();
    }

    /**
     * Returns all registered format names.
     *
     * @return unmodifiable collection of format names
     */
    public Collection<String> getFormatNames() {
        return formats.keySet();
    }

    /**
     * Routes a field to the best data format based on priority and capability.
     * The highest-priority plugin that supports the field wins.
     *
     * @param fieldType the mapped field type
     * @return the best matching DataFormat
     * @throws IllegalStateException if no format supports the field
     */
    public DataFormat routeField(MappedFieldType fieldType) {
        for (DataFormatPlugin plugin : priorityOrder) {
            if (plugin.supportsField(fieldType)) {
                return plugin.getDataFormat();
            }
        }
        throw new IllegalStateException("No data format supports field: [" + fieldType.name() + "] of type [" + fieldType.typeName() + "]");
    }

    /**
     * Builds a complete field-to-format routing table for an index mapping.
     * Called once per mapping change and cached on the IndexShard.
     *
     * @param mapperService the mapper service containing all field types
     * @return immutable map of field name to DataFormat
     */
    public Map<String, DataFormat> buildRoutingTable(MapperService mapperService) {
        Map<String, DataFormat> table = new HashMap<>();
        for (MappedFieldType fieldType : mapperService.fieldTypes()) {
            table.put(fieldType.name(), routeField(fieldType));
        }
        return Collections.unmodifiableMap(table);
    }
}
