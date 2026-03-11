/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a data format for storing and managing index data, with declared capabilities.
 * Each data format (e.g., Lucene, Parquet) declares what storage and query capabilities it supports.
 * Equality is based on the format name — there should be one DataFormat instance per unique name.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DataFormat {

    /** Well-known Lucene data format sentinel — used as the universal fallback. */
    public static final DataFormat LUCENE = new DataFormat("lucene", Set.of(), 0);

    /** Well-known metadata format sentinel — used for segment metadata files. */
    public static final DataFormat METADATA = new DataFormat("metadata", Set.of(), -1);

    /**
     * Returns a well-known DataFormat for the given name, or creates a bare sentinel instance.
     * This is intended for deserialization paths where only the format name is available.
     * Well-known names ("lucene", "metadata") return the canonical constant instances.
     *
     * @param name the data format name
     * @return the corresponding DataFormat instance
     */
    public static DataFormat of(String name) {
        if (LUCENE.name.equals(name)) return LUCENE;
        if (METADATA.name.equals(name)) return METADATA;
        return new DataFormat(name, Set.of(), 0);
    }

    /**
     * Capabilities that a data format can support.
     */
    @ExperimentalApi
    public enum Capability {
        /** Inverted index based full-text search (BM25, phrase queries) */
        FULL_TEXT_SEARCH,
        /** Column-oriented storage optimized for aggregations and analytics */
        COLUMNAR_STORAGE,
        /** Vector similarity search (kNN, ANN) */
        VECTOR_SEARCH,
        /** Numeric and date range queries via point trees */
        POINT_RANGE,
        /** Column-stride field data for sorting and scripting */
        DOC_VALUES,
        /** Original field value retrieval */
        STORED_FIELDS
    }

    private final String name;
    private final Set<Capability> capabilities;
    private final int priority;

    /**
     * Constructs a DataFormat with the given name, capabilities, and priority.
     *
     * @param name unique identifier for this format (e.g., "lucene", "parquet")
     * @param capabilities the set of capabilities this format supports
     * @param priority routing priority — higher values are preferred when multiple formats support a field
     */
    public DataFormat(String name, Set<Capability> capabilities, int priority) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.capabilities = Set.copyOf(capabilities);
        this.priority = priority;
    }

    /**
     * Returns the unique name of this data format.
     *
     * @return the format name
     */
    public String name() {
        return name;
    }

    /**
     * Checks if this format supports the given capability.
     *
     * @param capability the capability to check
     * @return true if supported
     */
    public boolean supports(Capability capability) {
        return capabilities.contains(capability);
    }

    /**
     * Returns all capabilities supported by this format.
     *
     * @return unmodifiable set of capabilities
     */
    public Set<Capability> capabilities() {
        return capabilities;
    }

    /**
     * Returns the routing priority. Higher values are preferred when multiple formats support a field.
     * Lucene should use 0 as the universal fallback.
     *
     * @return the priority value
     */
    public int priority() {
        return priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFormat that = (DataFormat) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "DataFormat{name='" + name + "', capabilities=" + capabilities + ", priority=" + priority + "}";
    }
}
