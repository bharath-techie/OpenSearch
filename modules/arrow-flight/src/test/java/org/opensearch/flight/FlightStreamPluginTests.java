/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.arrow.StreamManager;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

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
            null,
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

    public void testGetStreamManager() {
        StreamManager streamManager = flightStreamPlugin.getStreamManager();
        assertNotNull(streamManager);
        assertTrue(streamManager instanceof FlightStreamManager);
    }

    public void testGetSettings() {
        List<Setting<?>> settingsList = flightStreamPlugin.getSettings();
        assertNotNull(settingsList);
        assertFalse(settingsList.isEmpty());
        assertTrue(settingsList.stream().anyMatch(setting -> setting.getKey().equals("plugins.flight.port")));
        assertTrue(settingsList.stream().anyMatch(setting -> setting.getKey().equals("plugins.flight.host")));
    }

    public void testCreateComponentsWithNullArguments() {
        Collection<Object> components = flightStreamPlugin.createComponents(
            null,
            null,
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
        List<Setting<?>> settingsList = flightStreamPlugin.getSettings();
        Optional<Setting<?>> portSetting = settingsList.stream()
            .filter(setting -> setting.getKey().equals("plugins.flight.port"))
            .findFirst();
        Optional<Setting<?>> hostSetting = settingsList.stream()
            .filter(setting -> setting.getKey().equals("plugins.flight.host"))
            .findFirst();

        assertTrue(portSetting.isPresent());
        assertTrue(hostSetting.isPresent());
        assertEquals(8980, portSetting.get().getDefault(Settings.EMPTY));
        assertEquals("127.0.0.1", hostSetting.get().getDefault(Settings.EMPTY));
    }
}
