/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.arrow.StreamManager;
import org.opensearch.common.settings.Settings;
import org.apache.arrow.flight.FlightClient;

public class FlightServiceTests extends OpenSearchTestCase {
/*
    private FlightService flightService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().build();
        flightService = new FlightService(settings);
    }

    public void testGetStreamManager() {
        StreamManager streamManager = flightService.getStreamManager();
        assertNotNull(streamManager);
        assertTrue(streamManager instanceof FlightStreamManager);
    }

    public void testDoStart() throws Exception {
        flightService.doStart();
        assertTrue(flightService.isStarted());
        assertNotNull(flightService.getFlightServer());
    }

    public void testDoStop() throws Exception {
        flightService.doStart();
        flightService.doStop();
        assertFalse(flightService.isStarted());
        assertNull(flightService.getFlightServer());
    }

    public void testDoClose() throws Exception {
        flightService.doStart();
        flightService.doClose();
        assertFalse(flightService.isStarted());
        assertNull(flightService.getFlightServer());
        assertTrue(flightService.getStreamManager().getStreamProviders().isEmpty());
    }

    public void testCreateFlightClient() {
        FlightClient client = flightService.createFlightClient();
        assertNotNull(client);
        assertTrue(client.isRunning());
        assertEquals(FlightService.DEFAULT_FLIGHT_HOST, client.getLocation().getUri().getHost());
        assertEquals(FlightService.DEFAULT_FLIGHT_PORT, client.getLocation().getUri().getPort());
    }

    public void testCreateFlightClientWithCustomSettings() {
        Settings customSettings = Settings.builder()
            .put("plugins.flight.host", "custom-host")
            .put("plugins.flight.port", 1234)
            .build();
        FlightService customFlightService = new FlightService(customSettings);

        FlightClient client = null; // customFlightService.createFlightClient();
        assertNotNull(client);
        // Add more specific assertions based on the expected configuration of the FlightClient
    }

 */
}
