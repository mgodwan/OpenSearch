/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assume;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class TestDataFusionServiceTests extends OpenSearchTestCase {

    private DataFusionService service;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        service = new DataFusionService();
        service.doStart();
    }

    @Test
    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        // The service returns codec information in JSON format
        assertTrue("Version should contain codecs", version.contains("codecs"));
        assertTrue("Version should contain CsvDataSourceCodec", version.contains("CsvDataSourceCodec"));
    }

    @Test
    public void testCreateAndCloseContext() {
        service.registerDirectory("/somedir", List.of("some.csv"));
        long contextId = service.createSessionContext().join();
        // Create context
        assertTrue(contextId > 0);

        service.getVersion();
    }
}
