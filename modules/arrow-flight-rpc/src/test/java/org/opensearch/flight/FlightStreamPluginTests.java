/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;

import static org.mockito.Mockito.mock;

public class FlightStreamPluginTests extends OpenSearchTestCase {

    private Settings settings;
    private FlightStreamPlugin flightStreamPlugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().build();
        flightStreamPlugin = new FlightStreamPlugin(settings);
    }

    public void testCreateComponents() {
        Collection<Object> components = flightStreamPlugin.createComponents(
            null,
            mock(ClusterService.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertNotNull(components);
        assertTrue(components.stream().anyMatch(component -> component instanceof FlightService));
    }

    public void testGetStreamManager() {}

    public void testGetSettings() {}

    public void testCreateComponentsWithNullArguments() {
        Collection<Object> components = flightStreamPlugin.createComponents(
            null,
            mock(ClusterService.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertNotNull(components);
        assertFalse(components.isEmpty());
    }

    public void testGetSettingsDefaultValues() {

    }
}
