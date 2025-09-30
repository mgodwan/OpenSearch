/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.format;

import java.util.List;

/**
 * Directory abstraction for usage across reading/writing
 * data across different formats, and engines to be used in OpenSearch.
 */
public interface Directory {

    /**
     * @return path in the file system to the directory
     */
    String path();

    /**
     * List all files in a directory, relative to the directory path
     */
    List<String> listAll();
}
